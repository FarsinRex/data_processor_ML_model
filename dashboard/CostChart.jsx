const { useState, useEffect, useRef } = React;

function CostChart() {
    const chartRef = useRef(null);
    const chartInstance = useRef(null);
    const [chartData, setChartData] = useState({ labels: [], costs: [] });
    
    // function to fetch historical batch data
    useEffect(() => {
        fetchBatchHistory();
        const interval = setInterval(fetchBatchHistory, 10000); // Update every 10s
        return () => clearInterval(interval);
    }, []);
    
    // function to create/update chart when data changes
    useEffect(() => {
        if (chartRef.current && chartData.labels.length > 0) {
            createChart();
        }
        
        return () => {
            if (chartInstance.current) {
                chartInstance.current.destroy();
            }
        };
    }, [chartData]);
    
    async function fetchBatchHistory() {
        try {
            // fetch recent batches from database
            const response = await fetch('http://localhost:8000/batches/recent?limit=20');
            const batches = await response.json();
            
            if (batches && batches.length > 0) {
                const labels = batches.map((b, i) => `Batch ${b.id}`);
                const costs = batches.map(b => b.processing_cost);
                
                setChartData({ labels, costs });
            }
        } catch (error) {
            console.error('Error fetching batch history:', error);
        }
    }
    
    function createChart() {
        const ctx = chartRef.current.getContext('2d');
        
        if (chartInstance.current) {
            chartInstance.current.destroy();
        }
        
        chartInstance.current = new Chart(ctx, {
            type: 'line',
            data: {
                labels: chartData.labels,
                datasets: [{
                    label: 'Cost per Batch ($)',
                    data: chartData.costs,
                    borderColor: 'rgb(147, 51, 234)', // Purple
                    backgroundColor: 'rgba(147, 51, 234, 0.1)',
                    borderWidth: 2,
                    tension: 0.4,
                    fill: true,
                    pointRadius: 4,
                    pointHoverRadius: 6,
                    pointBackgroundColor: 'rgb(147, 51, 234)',
                    pointBorderColor: '#fff',
                    pointBorderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        labels: {
                            color: '#fff',
                            font: { size: 14 }
                        }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(0, 0, 0, 0.8)',
                        titleColor: '#fff',
                        bodyColor: '#fff',
                        borderColor: 'rgb(147, 51, 234)',
                        borderWidth: 1,
                        padding: 12,
                        displayColors: false,
                        callbacks: {
                            label: function(context) {
                                return `Cost: $${context.parsed.y.toFixed(3)}`;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            color: '#9ca3af',
                            callback: function(value) {
                                return '$' + value.toFixed(2);
                            }
                        },
                        grid: {
                            color: 'rgba(255, 255, 255, 0.1)'
                        }
                    },
                    x: {
                        ticks: {
                            color: '#9ca3af',
                            maxRotation: 45,
                            minRotation: 45
                        },
                        grid: {
                            color: 'rgba(255, 255, 255, 0.1)'
                        }
                    }
                }
            }
        });
    }
    
    return (
        <div className="bg-gray-800 rounded-lg shadow-lg p-6 mx-6 mb-6">
            <h2 className="text-xl font-bold mb-4 text-white">
                Cost Trends Over Time
            </h2>
            <div className="h-80">
                <canvas ref={chartRef}></canvas>
            </div>
        </div>
    );
}

window.CostChart = CostChart;