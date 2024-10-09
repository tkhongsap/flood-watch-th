def get_main_custom_css():
    return """
    <style>
    .stApp {
        background-color: #f8f9fa;
    }
    .gradient-text {
        font-weight: bold;
        background: linear-gradient(to right, #000080, #0000FF);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        display: inline;
        font-size: 2.5em;
    }
    .subheader {
        font-size: 18px !important;
        color: #5D6D7E !important;
        text-align: center;
        margin-bottom: 40px;
    }
    .agent-card {
        background-color: white;
        border: 1px solid #E5E8E8;
        border-radius: 10px;
        padding: 20px;
        margin: 10px;
        text-align: center;
        height: 100%;
        display: flex;
        flex-direction: column;
        align-items: center;
    }
    .agent-icon {
        width: 90px;
        height: 90px;
        margin-bottom: 15px;
    }
    .agent-title {
        font-size: 18px;
        font-weight: bold;
        color: #2E4053;
        margin-bottom: 5px;
    }
    .agent-title-training {
        color: #A9A9A9;  /* Gray color for products in training */
    }
    .agent-author {
        font-size: 14px;
        color: #5D6D7E;
        margin-bottom: 10px;
    }
    .agent-stats {
        font-size: 12px;
        color: #7F8C8D;
        margin-bottom: 15px;
    }
    .top-menu {
        display: flex;
        justify-content: flex-end;
        padding: 10px;
        background-color: #2C3E50;
    }
   .menu-item {
        color: white;
        text-decoration: none;
        margin-left: 20px;
        font-size: 16px;
    }
    .stButton {
        display: flex;
        justify-content: center;
        width: 100%;
    }
    .stButton > button {
        width: 50%;
        padding: 0.4rem 0.8rem;
        font-size: 0.9rem;
        font-weight: normal;
    }
    .agent-content {
        flex-grow: 1;
        width: 100%;
    }
    .agent-button {
        width: 100%;
        display: flex;
        justify-content: center;
        margin-top: 1rem;
    }
    </style>
    """