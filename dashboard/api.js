// api.js  - API communication helper
const API_BASE = 'http://localhost:8000';

const api = {
    // fetch database statistics
    async getDatabaseStats() {
        const response = await fetch(`${API_BASE}/database/stats`);
        return response.json();

    },

    // fetch worker statistics
    async getWorkerStats() {
        const response = await fetch(`${API_BASE}/worker/stats`);
        return response.json();
    },

    //start simulator
    async startSimulator() {
        const response = await fetch(`${API_BASE}/simulator/start`, 
            { method: 'POST'

             });
            return response.json();
    }
};

window.api = api;