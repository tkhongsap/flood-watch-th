def get_flood_alert_banner():
    return """
    <style>
    .main-header {
        background: linear-gradient(135deg, #0072ff, #00c6ff); /* Light blue gradient to represent water */
        padding: 1rem;  /* Reduced padding */
        border-radius: 15px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 1.0rem;  /* Reduced margin */
        text-align: center;
    }
    .main-header h1 {
        color: white;
        font-size: 2.0rem;  /* Slightly smaller font size */
        margin: 0;
        text-align: center;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.4);  /* Adjusted shadow intensity */
    }
    .main-header p {
        color: #f0f0f0;
        font-size: 0.9rem;  /* Slightly smaller font size */
        margin: 0.3rem 0 0 0;  /* Adjusted margin */
        text-align: center;
    }
    .benefit-pills {
        display: flex;
        justify-content: center;
        flex-wrap: wrap;
        gap: 0.3rem;  /* Reduced gap between pills */
        margin-top: 0.6rem;  /* Reduced margin */
    }
    .benefit-pill {
        background-color: rgba(255, 255, 255, 0.15);  /* Slightly lighter background */
        color: white;
        padding: 0.3rem 0.5rem;  /* Reduced padding */
        border-radius: 20px;
        font-size: 0.85rem;  /* Slightly smaller font size */
        backdrop-filter: blur(5px);
    }
    </style>
    <div class="main-header">
        <h1>🌊 Flood Monitoring & Alerts</h1>
        <p>Stay informed with real-time water level updates and proactive flood risk alerts.</p>
        <div class="benefit-pills">
            <span class="benefit-pill">📡 Latest Data</span>
            <span class="benefit-pill">⚠️ Proactive Risk Alerts</span>
            <span class="benefit-pill">🌍 Location-Specific Insights</span>
        </div>
    </div>
    """
