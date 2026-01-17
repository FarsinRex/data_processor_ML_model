import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, Tuple, Optional
import pickle
import os

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.preprocessing import StandardScaler

from db_glue import Database

class BatchOptimizer:
    """
    ML powered batch size optimizer for streaming events.
    Learns from historical batch processing data to predict optimal
    batch sizes that minimizes cost per event
    """
    def __init__(self):
        self.db = Database()
        self.model = None
        self.scaler = StandardScaler()
        self.is_trained = False
        
        #Model save path
        self.model_path = "batch_optimizer_model.pkl"
        self.scaler_path = "batch_optimizer_scaler.pkl"
        print("BatchOptimizer initialized.")
        
    def fetch_training_data(self) -> pd.DataFrame:
        
        """
        Fetch historical batch data from database
        Returns:
        Dataframe with batch processing history
        """
        query = """
        SELECT 
                b.id as batch_id,
                b.batch_size,
                b.total_data_size_kb,
                b.processing_time_seconds,
                b.processing_cost,
                b.started_at,
                cm.cost_per_event
            FROM batches b
            JOIN cost_metrics cm ON b.id = cm.batch_id
            WHERE b.status = 'completed'
            ORDER BY b.started_at;
        """
        results = self.db.execute_query(query, fetch=True)
        
        if not results:
            print("no training data found in database.")
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        print(f"fetched {len(df)} records for training.")
       
        return df
    
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create features and target variable from raw data
        feature engineering is crucial for model performance
        selecting meaningful features that help the model learn patterns
        Args:
            df: Raw dataframe from database
        Returns:
            Dataframe with features and target variable
        """
        #Feature engineering
        if df.empty:
            return df
        #Feature 1: Average data per event
        df['avg_data_size_per_event'] = df['total_data_size_kb'] / df['batch_size']
        
        #feature 2: Processing speed (kb/sec)
        df['processing_speed_kb_per_sec'] = (
            df['total_data_size_kb'] / df['processing_time_seconds']
        )
        
        #feature 3: Time based features
        df['started_at'] = pd.to_datetime(df['started_at'])
        df['hour_of_day'] = df['started_at'].dt.hour
        df['day_of_week'] = df['started_at'].dt.dayofweek
    
        #feature 4: Batch efficiency metric(lower is better)
        df['efficiency'] = df['cost_per_event']
        
        #feature 5: Data intensity (total data/batch size)
        df['data_intensity'] = df['total_data_size_kb'] / df['batch_size']
        
        print("features engineered.")
        print(f"   - avg_data_per_event")
        print(f"   - processing_speed_kb_per_sec")
        print(f"   - hour_of_day, day_of_week")
        print(f"   - efficiency (cost_per_event)")
        print(f"   - data_intensity")
        
        return df
    
    def prepairing_training_data(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        """
        Prepare feature matrix X and target vector y for training
        Args:
            df: Dataframe with engineered features
        Returns:
        Tuple of feature matrix X and target vector y
        """
        #select features from training
        feature_columns = [
            'total_data_size_kb',
            'avg_data_size_per_event',
            'hour_of_day',
            'processing_time_seconds',
            'cost_per_event'
        ]
        
        X = df[feature_columns].values
        y = df['batch_size'].values
        
        print(f"training data shape: Features(X) = {X.shape}, Target(y) = {y.shape}")
        return X,y
    
    def train_model(self, test_size: float = 0.2) -> Dict:
        """
        Train the model of historical data
        
        Args: 
        test_size = Fraction of data used for testing
        
        Returns: 
        Dictionary with training metrics
        """
        print("\n"+"="*60)
        print("starting model training")
        print("="*60)
        
        #step1: fetch data
        df = self.fetch_training_data()
        
        if df.empty or len(df) < 60:
            print("not enough data to train model.")
            print("Info: Run the system longer to collect more data")
            return {
                'success': False,
                'message': 'not enough data to train model'
            }
        
        #step 2 engineer features
        df = self.engineer_features(df)
        
        #step3 prepare training data
        X,y = self.prepairing_training_data(df)
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42
        )
        print(f"\n Data split")
        print(f" Training samples: {len(X_train)}")
        print(f" Testing samples: {len(X_test)}")
        
        #step5: Scale features to normalize features to similar ranges
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        #step6: train the model (Randomforestregressor)
        print("\n  Training random forest model")
        
        self.model  = RandomForestRegressor(
            n_estimators=100,#number of trees
            max_depth=10, #max tree depth
            min_samples_split=2, #min samples to split node
            random_state=42, #to keep the random state configuration in a key
            n_jobs=1 #use all CPU cores
        )
        self.model.fit(X_train_scaled, y_train)
        
        #evaluate model
        y_pred_train = self.model.predict(X_train_scaled)
        y_pred_test = self.model.predict(X_test_scaled)
        
        #calculate metrics
        train_r2 = r2_score(y_train, y_pred_train)
        test_r2 = r2_score(y_test, y_pred_test)
        train_mae = mean_absolute_error(y_train, y_pred_train)
        test_mae = mean_absolute_error(y_test, y_pred_test)
        train_rmse = np.sqrt(mean_squared_error(y_train, y_pred_train))
        test_rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))
        
        
        #feature importance
        feature_names = [
            'total_data_kb',
            'avg_data_per_event',
            'hour_of_day',
            'processing_time_seconds',
            'cost_per_event'
        ]
        feature_importance = dict(zip(feature_names, 
                                       self.model.feature_importances_
        ))
        self.is_trained = True
        
        print("\n Model training completed.")
        print("="*60)
        print(f"\n  Performance Metrics:")
        print(f"   R² Score (Train): {train_r2:.3f}")
        print(f"   R² Score (Test):  {test_r2:.3f}")
        print(f"   MAE (Train): {train_mae:.2f} events")
        print(f"   MAE (Test):  {test_mae:.2f} events")
        print(f"   RMSE (Train): {train_rmse:.2f} events")
        print(f"   RMSE (Test):  {test_rmse:.2f} events")
        
        print(f"\n Feature Importances:")
        for feature, importance in sorted(
            feature_importance.items(),
            key=lambda x: x[1],
            reverse=True
        ):
            
            print(f"   {feature:20s}: {importance:.3f}")
        
        print("="*60 + "\n")
        
        return {
            'success': True,
            'train_r2': train_r2,
            'test_r2': test_r2,
            'train_mae': train_mae,
            'test_mae': test_mae,
            'train_rmse': train_rmse,
            'test_rmse': test_rmse,
            'feature_importance': feature_importance,
            'training_samples': len(X_train),
            'test_samples': len(X_test)
        }
    
    def predict_optimal_batch_size(
        self,
        total_data_kb: float,
        avg_data_per_event: float,
        hour_of_day: int,
        last_processing_time: float,
        last_cost_per_event: float
    ) -> int:
        """
        Predict optimal batch size for current conditions.
        
        Args:
            total_data_kb: Total data waiting to be processed
            avg_data_per_event: Average data size per event
            hour_of_day: Current hour (0-23)
            last_processing_time: How long last batch took
            last_cost_per_event: Cost efficiency of last batch
            
        Returns:
            Predicted optimal batch size (rounded to nearest 10)
        """
        if not self.is_trained:
            print("  Model not trained yet, using default batch size: 50")
            return 50
        
        # Prepare input features
        features = np.array([[
            total_data_kb,
            avg_data_per_event,
            hour_of_day,
            last_processing_time,
            last_cost_per_event
        ]])
        
        # Scale features
        features_scaled = self.scaler.transform(features)
        
        # Predict
        predicted_size = self.model.predict(features_scaled)[0]
        
        # Round to nearest 10 and constrain to reasonable range
        predicted_size = round(predicted_size / 10) * 10
        predicted_size = max(20, min(200, predicted_size))
        
        return int(predicted_size)
    
    def save_model(self):
        """Save trained model and scaler to disk."""
        if not self.is_trained:
            print(" No trained model to save")
            return
        
        with open(self.model_path, 'wb') as f:
            pickle.dump(self.model, f)
        
        with open(self.scaler_path, 'wb') as f:
            pickle.dump(self.scaler, f)
        
        print(f" Model saved to {self.model_path}")
        print(f" Scaler saved to {self.scaler_path}")
    
    def load_model(self) -> bool:
        """
        Load trained model from disk.
        
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(self.model_path):
            print(f" Model file not found: {self.model_path}")
            return False
        
        try:
            with open(self.model_path, 'rb') as f:
                self.model = pickle.load(f)
            
            with open(self.scaler_path, 'rb') as f:
                self.scaler = pickle.load(f)
            
            self.is_trained = True
            print(" Model loaded successfully")
            return True
        
        except Exception as e:
            print(f" Error loading model: {e}")
            return False


# Test the ML model
if __name__ == "__main__":
    print(" Testing ML Model")
    print("="*60)
    
    optimizer = BatchOptimizer()
    
    # Train model
    results = optimizer.train_model()
    
    if results['success']:
        # Save model
        optimizer.save_model()
        
        # Test prediction
        print("\n  Testing Prediction:")
        predicted_batch = optimizer.predict_optimal_batch_size(
            total_data_kb=1500.0,
            avg_data_per_event=15.0,
            hour_of_day=14,
            last_processing_time=5.0,
            last_cost_per_event=0.007
        )
        
        print(f"   Predicted optimal batch size: {predicted_batch}")
        print("\n ML model ready for production!")
    else:
        print("\n Need more training data. Run your system for 5+ minutes first!")
        print("   Then run this script again.")
        
        
        