// App.jsx - Main application component

const { useState } = React;

function App() {
    return (
        <div className="min-h-screen bg-gray-900">
            {/* Header */}
            <header className="bg-gray-800 shadow-lg">
                <div className="container mx-auto px-6 py-4">
                    <h1 className="text-3xl font-bold text-white">
                    ML Data Pipeline Dashboard
                    </h1>
                    <p className="text-gray-400 mt-1">
                        Real-time monitoring & cost optimization
                    </p>
                </div>
            </header>
            
            {/* Stats Cards */}
            <main>
                <StatsCards />
                <CostChart />
                <BatchSizeChart />
            </main>
            
            {/* Footer */}
            <footer className="bg-gray-800 mt-12 py-4">
                <div className="container mx-auto px-6 text-center text-gray-400 text-sm">
                    ML-Powered Data Pipeline | Built with React + FastAPI + PostgreSQL
                </div>
            </footer>
        </div>
    );
}

// Render app
const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<App />);