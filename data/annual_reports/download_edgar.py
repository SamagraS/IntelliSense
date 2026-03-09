from sec_edgar_downloader import Downloader
from config import RAW_REPORT_DIR
import os

def download_10k(company_ticker, limit=5, email="itsaaryantime1091@gmail.com"):
    # Create american subfolder for US companies
    american_dir = os.path.join(RAW_REPORT_DIR, "american")
    
    dl = Downloader("IntelliSense", email, american_dir)

    dl.get(
        "10-K",
        company_ticker,
        limit=limit
    )


if __name__ == "__main__":
    companies = [
        # Technology
        "AAPL",   # Apple
        "MSFT",   # Microsoft
        "GOOGL",  # Alphabet (Google)
        "META",   # Meta (Facebook)
        "NVDA",   # NVIDIA
        "INTC",   # Intel
        "CSCO",   # Cisco
        "ORCL",   # Oracle
        "ADBE",   # Adobe
        "CRM",    # Salesforce
        "IBM",    # IBM
        "QCOM",   # Qualcomm
        "AMD",    # AMD
        "AVGO",   # Broadcom
        "TXN",    # Texas Instruments
        "SNOW",   # Snowflake
        "NOW",    # ServiceNow
        "PANW",   # Palo Alto Networks
        "AMAT",   # Applied Materials
        "MU",     # Micron Technology
        
        # E-commerce & Retail
        "AMZN",   # Amazon
        "WMT",    # Walmart
        "HD",     # Home Depot
        "TGT",    # Target
        "COST",   # Costco
        "LOW",    # Lowe's
        "NKE",    # Nike
        "SBUX",   # Starbucks
        "TJX",    # TJX Companies
        "EBAY",   # eBay
        
        # Financial Services
        "JPM",    # JPMorgan Chase
        "BAC",    # Bank of America
        "WFC",    # Wells Fargo
        "C",      # Citigroup
        "GS",     # Goldman Sachs
        "MS",     # Morgan Stanley
        "BLK",    # BlackRock
        "SCHW",   # Charles Schwab
        "AXP",    # American Express
        "V",      # Visa
        "MA",     # Mastercard
        "SPGI",   # S&P Global
        "CME",    # CME Group
        "USB",    # U.S. Bancorp
        "PNC",    # PNC Financial
        
        # Healthcare & Pharma
        "JNJ",    # Johnson & Johnson
        "UNH",    # UnitedHealth Group
        "PFE",    # Pfizer
        "ABBV",   # AbbVie
        "TMO",    # Thermo Fisher
        "LLY",    # Eli Lilly
        "MRK",    # Merck
        "ABT",    # Abbott Laboratories
        "DHR",    # Danaher
        "BMY",    # Bristol Myers Squibb
        "AMGN",   # Amgen
        "GILD",   # Gilead Sciences
        "CVS",    # CVS Health
        "CI",     # Cigna
        
        # Consumer Goods
        "PG",     # Procter & Gamble
        "KO",     # Coca-Cola
        "PEP",    # PepsiCo
        "MDLZ",   # Mondelez
        "CL",     # Colgate-Palmolive
        "KMB",    # Kimberly-Clark
        "GIS",    # General Mills
        "K",      # Kellogg
        
        # Automotive & Industrial
        "TSLA",   # Tesla
        "F",      # Ford
        "GM",     # General Motors
        "CAT",    # Caterpillar
        "DE",     # Deere & Company
        "BA",     # Boeing
        "HON",    # Honeywell
        "RTX",    # Raytheon Technologies
        "LMT",    # Lockheed Martin
        "GE",     # General Electric
        "MMM",    # 3M
        
        # Energy
        "XOM",    # ExxonMobil
        "CVX",    # Chevron
        "COP",    # ConocoPhillips
        "SLB",    # Schlumberger
        "EOG",    # EOG Resources
        
        # Telecom & Media
        "T",      # AT&T
        "VZ",     # Verizon
        "TMUS",   # T-Mobile
        "CMCSA",  # Comcast
        "DIS",    # Disney
        "NFLX",   # Netflix
        
        # Other Major Companies
        "UPS",    # UPS
        "FDX",    # FedEx
        "NEE",    # NextEra Energy
        "DUK",    # Duke Energy
    ]

    for c in companies:
        download_10k(c)