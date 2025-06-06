"""
FastAPI server for Stock Warehouse (Phase M4).

Provides REST API endpoints for accessing stock data.
"""

from datetime import date, datetime
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import pandas as pd

from .db_client import StockDB
from etl.config import get_settings


# Pydantic models for API responses
class AssetInfo(BaseModel):
    """Asset information response model."""
    asset_id: int
    symbol: str
    asset_type: str
    currency: str
    exchange: Optional[str]
    company_name: Optional[str]
    sector: Optional[str]
    is_active: bool


class PriceData(BaseModel):
    """Price data response model."""
    symbol: str
    price_date: date
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: Optional[int]
    adj_close_price: Optional[float]
    asset_type: str
    currency: str
    exchange: Optional[str]


class PriceSummary(BaseModel):
    """Price summary statistics model."""
    symbol: str
    trading_days: int
    min_price: Optional[float]
    max_price: Optional[float]
    avg_price: Optional[float]
    price_volatility: Optional[float]
    avg_volume: Optional[int]
    latest_price: Optional[float]
    start_date: Optional[date]
    end_date: Optional[date]
    analysis_days: int


class HealthCheck(BaseModel):
    """Health check response model."""
    status: str
    database_connected: bool
    asset_count: Optional[int] = None
    price_count: Optional[int] = None
    batch_count: Optional[int] = None
    latest_date: Optional[date] = None
    timestamp: str
    error: Optional[str] = None


# FastAPI app instance
app = FastAPI(
    title="Stock Warehouse API",
    description="REST API for accessing stock market data warehouse",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency to get database client
def get_db() -> StockDB:
    """Dependency to provide database client."""
    return StockDB()


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Stock Warehouse API",
        "version": "0.1.0",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health", response_model=HealthCheck, tags=["Health"])
async def health_check(db: StockDB = Depends(get_db)):
    """Database and API health check."""
    return db.health_check()


@app.get("/symbols", response_model=List[str], tags=["Assets"])
async def get_symbols(
    asset_type: Optional[str] = Query(None, description="Filter by asset type"),
    db: StockDB = Depends(get_db)
):
    """Get list of available symbols."""
    try:
        symbols = db.get_available_symbols(asset_type)
        return symbols
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/assets/{symbol}", response_model=AssetInfo, tags=["Assets"])
async def get_asset_info(
    symbol: str,
    db: StockDB = Depends(get_db)
):
    """Get detailed information about an asset."""
    try:
        asset_info = db.get_asset_info(symbol)
        if not asset_info:
            raise HTTPException(status_code=404, detail=f"Asset {symbol} not found")
        
        return AssetInfo(**asset_info)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/prices", response_model=List[PriceData], tags=["Prices"])
async def get_prices(
    symbols: List[str] = Query(..., description="List of symbols"),
    start_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: date = Query(..., description="End date (YYYY-MM-DD)"),
    db: StockDB = Depends(get_db)
):
    """Get price data for symbols within date range."""
    try:
        if end_date < start_date:
            raise HTTPException(status_code=400, detail="End date must be after start date")
        
        df = db.prices(symbols, start_date, end_date)
        
        if df.empty:
            return []
        
        # Convert DataFrame to list of PriceData models
        prices = []
        for _, row in df.iterrows():
            prices.append(PriceData(
                symbol=row['symbol'],
                price_date=row['price_date'],
                open_price=row['open_price'],
                high_price=row['high_price'],
                low_price=row['low_price'],
                close_price=row['close_price'],
                volume=row['volume'],
                adj_close_price=row['adj_close_price'],
                asset_type=row['asset_type'],
                currency=row['currency'],
                exchange=row['exchange']
            ))
        
        return prices
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/prices/{symbol}/summary", response_model=PriceSummary, tags=["Prices"])
async def get_price_summary(
    symbol: str,
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    db: StockDB = Depends(get_db)
):
    """Get price summary statistics for a symbol."""
    try:
        summary = db.get_price_summary(symbol, days)
        
        if 'error' in summary:
            raise HTTPException(status_code=404, detail=summary['error'])
        
        return PriceSummary(**summary)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/prices/latest", response_model=List[Dict[str, Any]], tags=["Prices"])
async def get_latest_prices(
    symbols: Optional[List[str]] = Query(None, description="List of symbols"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    db: StockDB = Depends(get_db)
):
    """Get latest available prices."""
    try:
        df = db.get_latest_prices(symbols, limit)
        
        if df.empty:
            return []
        
        return df.to_dict('records')
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/date-range", tags=["Metadata"])
async def get_date_range(
    symbol: Optional[str] = Query(None, description="Symbol to check date range"),
    db: StockDB = Depends(get_db)
):
    """Get available date range for data."""
    try:
        date_range = db.get_date_range(symbol)
        return date_range
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Future endpoints for Phase M2 and beyond
@app.get("/dividends/{symbol}", tags=["Corporate Actions"])
async def get_dividends(symbol: str):
    """Get dividend history for a symbol (Phase M2)."""
    raise HTTPException(status_code=501, detail="Dividends endpoint not yet implemented")


@app.get("/splits/{symbol}", tags=["Corporate Actions"])
async def get_splits(symbol: str):
    """Get stock split history for a symbol (Phase M2)."""
    raise HTTPException(status_code=501, detail="Stock splits endpoint not yet implemented")


@app.get("/intraday/{symbol}", tags=["Intraday"])
async def get_intraday_prices(symbol: str):
    """Get intraday price data (Phase M3)."""
    raise HTTPException(status_code=501, detail="Intraday data endpoint not yet implemented")


# Development server runner
if __name__ == "__main__":
    import uvicorn
    
    settings = get_settings()
    uvicorn.run(
        "app.fastapi_server:app",
        host=settings.api_host,
        port=settings.api_port,
        workers=settings.api_workers,
        reload=True
    ) 