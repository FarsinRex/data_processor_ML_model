// StatsCards.jsx - Real-time stats display

const { useState, useEffect } = React;

function StatsCards() {
    const [stats, setStats] = useState({
        events: { total: 0, processed: 0, unprocessed: 0 },
        batches: { total: 0 },
        costs: { total: 0 },
        worker: { ml_predictions_used: 0, is_running: false }
    });
    
    const [loading, setLoading] = useState(true);
    
    // Fetch stats on mount and every 5 seconds
    useEffect(() => {
    fetchStats();
    const interval = setInterval(() => {
        if (document.visibilityState === 'visible') {
            fetchStats();
        }
    }, 5000);
    
    return () => clearInterval(interval);
    }, []);
    
    async function fetchStats() {
        try {
            const [dbStats, workerStats] = await Promise.all([
                api.getDatabaseStats(),
                api.getWorkerStats()
            ]);
            
            setStats({
                events: dbStats.events,
                batches: dbStats.batches,
                costs: dbStats.costs,
                worker: workerStats
            });
            
            setLoading(false);
        } catch (error) {
            console.error('Error fetching stats:', error);
        }
    }
    
    if (loading) {
        return (
            <div className="flex justify-center items-center h-64">
                <div className="text-xl">Loading stats...</div>
            </div>
        );
    }
    
    return (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 p-6">
            {/* Card 1: Events */}
            <div className="bg-gradient-to-br from-blue-500 to-blue-700 rounded-lg shadow-lg p-6">
                <div className="flex items-center justify-between">
                    <div>
                        <p className="text-blue-100 text-sm font-medium">Total Events</p>
                        <p className="text-3xl font-bold mt-2">{stats.events.total.toLocaleString()}</p>
                    </div>
                    <div className="text-5xl opacity-50">📊</div>
                </div>
                <div className="mt-4 pt-4 border-t border-blue-400">
                    <div className="flex justify-between text-sm">
                        <span className="text-blue-100">Processed:</span>
                        <span className="font-semibold">{stats.events.processed}</span>
                    </div>
                    <div className="flex justify-between text-sm mt-1">
                        <span className="text-blue-100">Queue:</span>
                        <span className="font-semibold">{stats.events.unprocessed}</span>
                    </div>
                </div>
            </div>
            
            {/* Card 2: Batches */}
            <div className="bg-gradient-to-br from-green-500 to-green-700 rounded-lg shadow-lg p-6">
                <div className="flex items-center justify-between">
                    <div>
                        <p className="text-green-100 text-sm font-medium">Batches Processed</p>
                        <p className="text-3xl font-bold mt-2">{stats.batches.total}</p>
                    </div>
                    <div className="text-5xl opacity-50">📦</div>
                </div>
                <div className="mt-4 pt-4 border-t border-green-400">
                    <div className="flex justify-between text-sm">
                        <span className="text-green-100">Avg Size:</span>
                        <span className="font-semibold">{stats.batches.avg_size || 0} events</span>
                    </div>
                    <div className="flex justify-between text-sm mt-1">
                        <span className="text-green-100">Avg Time:</span>
                        <span className="font-semibold">{(stats.batches.avg_processing_time || 0).toFixed(1)}s</span>
                    </div>
                </div>
            </div>
            
            {/* Card 3: Costs */}
            <div className="bg-gradient-to-br from-purple-500 to-purple-700 rounded-lg shadow-lg p-6">
                <div className="flex items-center justify-between">
                    <div>
                        <p className="text-purple-100 text-sm font-medium">Total Cost</p>
                        <p className="text-3xl font-bold mt-2">${stats.costs.total.toFixed(2)}</p>
                    </div>
                    <div className="text-5xl opacity-50">💰</div>
                </div>
                <div className="mt-4 pt-4 border-t border-purple-400">
                    <div className="flex justify-between text-sm">
                        <span className="text-purple-100">Per Event:</span>
                        <span className="font-semibold">${(stats.costs.per_event || 0).toFixed(4)}</span>
                    </div>
                    <div className="flex justify-between text-sm mt-1">
                        <span className="text-purple-100">Avg Batch:</span>
                        <span className="font-semibold">${(stats.batches.avg_cost || 0).toFixed(3)}</span>
                    </div>
                </div>
            </div>
            
            {/* Card 4: ML Status */}
            <div className="bg-gradient-to-br from-pink-500 to-pink-700 rounded-lg shadow-lg p-6">
                <div className="flex items-center justify-between">
                    <div>
                        <p className="text-pink-100 text-sm font-medium">ML Predictions</p>
                        <p className="text-3xl font-bold mt-2">{stats.worker.ml_predictions_used || 0}</p>
                    </div>
                    <div className="text-5xl opacity-50">🧠</div>
                </div>
                <div className="mt-4 pt-4 border-t border-pink-400">
                    <div className="flex justify-between text-sm">
                        <span className="text-pink-100">Status:</span>
                        <span className={`font-semibold ${stats.worker.is_running ? 'text-green-300' : 'text-red-300'}`}>
                            {stats.worker.is_running ? '● RUNNING' : '○ STOPPED'}
                        </span>
                    </div>
                    <div className="flex justify-between text-sm mt-1">
                        <span className="text-pink-100">ML Enabled:</span>
                        <span className="font-semibold">
                            {stats.worker.ml_enabled ? '✓ Yes' : '✗ No'}
                        </span>
                    </div>
                </div>
            </div>
        </div>
    );
}

// Make available globally
window.StatsCards = StatsCards;