// BatchSizeChart.jsx - To visualize ML predictions

const { useState, useEffect, useRef } = React;

function BatchSizeChart() {
    const chartRef = useRef(null);
    const chartInstance = useRef(null);
    const [chartData, setChartData] = useState({ 
        labels: [], 
        batchSizes: [],
        avgCosts: []
    });
    
    useEffect(() => {
        fetchBatchSizeData();
        const interval = setInterval(fetchBatchSizeData, 15000);
        return () => clearInterval(interval);
    }, []);
    
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
    
    async function fetchBatchSizeData() {
        try {
            const response = await fetch('http://localhost:8000/batches/recent?limit=15');
            const batches = await response.json();
            
            if (batches && batches.length > 0) {
                const labels = batches.map((b, i) => `#${b.id}`);
                const batchSizes = batches.map(b => b.batch_size);
                const avgCosts = batches.map(b => (b.processing_cost / b.batch_size * 1000).toFixed(2)); // Cost per event in cents
                
                setChartData({ labels, batchSizes, avgCosts });
            }
        } catch (error) {
            console.error('Error fetching batch size data:', error);
        }
    }
    
    function createChart() {
        const ctx = chartRef.current.getContext('2d');
        
        if (chartInstance.current) {
            chartInstance.current.destroy();
        }
        
        chartInstance.current = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: chartData.labels,
                datasets: [
                    {
                        label: 'Batch Size (events)',
                        data: chartData.batchSizes,
                        backgroundColor: 'rgba(59, 130, 246, 0.8)', // Blue
                        borderColor: 'rgb(59, 130, 246)',
                        borderWidth: 2,
                        yAxisID: 'y',
                        order: 2
                    },
                    {
                        label: 'Cost Efficiency (¢/event)',
                        data: chartData.avgCosts,
                        type: 'line',
                        borderColor: 'rgb(34, 197, 94)', // Green
                        backgroundColor: 'rgba(34, 197, 94, 0.1)',
                        borderWidth: 3,
                        tension: 0.4,
                        fill: true,
                        yAxisID: 'y1',
                        order: 1,
                        pointRadius: 5,
                        pointHoverRadius: 7,
                        pointBackgroundColor: 'rgb(34, 197, 94)',
                        pointBorderColor: '#fff',
                        pointBorderWidth: 2
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false,
                },
                plugins: {
                    legend: {
                        display: true,
                        position: 'top',
                        labels: {
                            color: '#fff',
                            font: { size: 12 },
                            usePointStyle: true,
                            padding: 15
                        }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(0, 0, 0, 0.8)',
                        titleColor: '#fff',
                        bodyColor: '#fff',
                        borderColor: 'rgb(59, 130, 246)',
                        borderWidth: 1,
                        padding: 12,
                        displayColors: true
                    }
                },
                scales: {
                    y: {
                        type: 'linear',
                        display: true,
                        position: 'left',
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Batch Size (events)',
                            color: '#9ca3af',
                            font: { size: 12 }
                        },
                        ticks: {
                            color: '#9ca3af'
                        },
                        grid: {
                            color: 'rgba(255, 255, 255, 0.1)'
                        }
                    },
                    y1: {
                        type: 'linear',
                        display: true,
                        position: 'right',
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Cost per Event (cents)',
                            color: '#9ca3af',
                            font: { size: 12 }
                        },
                        ticks: {
                            color: '#9ca3af'
                        },
                        grid: {
                            drawOnChartArea: false,
                        }
                    },
                    x: {
                        ticks: {
                            color: '#9ca3af'
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
            <div className="flex items-center justify-between mb-4">
                <h2 className="text-xl font-bold text-white">
                    ML Batch Size Optimization
                </h2>
                <span className="text-sm text-gray-400">
                    Showing last 15 batches
                </span>
            </div>
            <div className="h-80">
                <canvas ref={chartRef}></canvas>
            </div>
            <div className="mt-4 p-4 bg-gray-700 rounded-lg">
                <p className="text-sm text-gray-300">
                    <span className="font-semibold text-blue-400">Blue bars</span> show batch sizes chosen by ML model. 
                    <span className="font-semibold text-green-400 ml-2">Green line</span> shows cost efficiency - 
                    ML adapts batch size to minimize cost per event.
                </p>
            </div>
        </div>
    );
}

window.BatchSizeChart = BatchSizeChart;