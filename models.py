from sqlalchemy import Column, Numeric, Integer, String, Date, DateTime, func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class SpimexTradingResults(Base):
    __tablename__ = "spimex_trading_results"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_product_id = Column(String(20), nullable=False)
    exchange_product_name = Column(String(255))
    oil_id = Column(String(4), nullable=False)
    delivery_basis_id = Column(String(3), nullable=False)
    delivery_basis_name = Column(String(255))
    delivery_type_id = Column(String(1), nullable=False)
    volume = Column(Numeric(15, 2))
    total = Column(Numeric(20, 2))
    count = Column(Integer)
    date = Column(Date, nullable=False)
    created_on = Column(DateTime, default=func.now())
    updated_on = Column(DateTime, default=func.now(), onupdate=func.now())