import os
import pytest
import asyncio
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from source.classes import FuturesProcessor, FuturesTrade

# Load environment variables
load_dotenv()

Base = declarative_base()


@pytest.fixture(scope="function")
def db_engine():
    engine = create_engine(os.getenv('ENGINE'))
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def db_session(db_engine):
    Session = sessionmaker(bind=db_engine)
    session = Session()
    yield session
    session.query(FuturesTrade).delete()
    session.commit()
    session.close()


@pytest.mark.asyncio
async def test_initialize_futures_processor(db_engine):
    processor = FuturesProcessor("ethusdt")
    assert processor.symbol == "ethusdt"
    assert str(processor.engine.url) == str(db_engine.url)
    assert processor.session is not None


@pytest.mark.asyncio
async def test_handle_trade(db_session):
    # Очистка таблицы перед тестом
    db_session.query(FuturesTrade).delete()
    db_session.commit()

    processor = FuturesProcessor("ethusdt")

    trade_data = {
        "s": "ETHUSDT",
        "p": "2000.00"
    }

    await processor.handle_trade(json.dumps(trade_data))

    trades = db_session.query(FuturesTrade).filter_by(symbol="ETHUSDT").all()
    assert len(trades) == 1
    assert float(trades[0].price) == 2000.00


@pytest.mark.asyncio
async def test_delete_old_trades(db_session):
    processor = FuturesProcessor("ethusdt")

    old_trade = FuturesTrade(
        symbol="ETHUSDT",
        price=2000.00,
        timestamp=datetime.now() - timedelta(minutes=61)
    )
    new_trade = FuturesTrade(
        symbol="ETHUSDT",
        price=2000.00,
        timestamp=datetime.now()
    )

    db_session.add(old_trade)
    db_session.add(new_trade)
    db_session.commit()

    await processor.delete_old_trades()

    trades = db_session.query(FuturesTrade).filter_by(symbol="ETHUSDT").all()
    assert len(trades) == 1
    assert trades[0].timestamp > datetime.now() - timedelta(minutes=60)


@pytest.mark.asyncio
async def test_read_data_to_dataframe(db_session):
    processor = FuturesProcessor("ethusdt")

    trade = FuturesTrade(
        symbol="ETHUSDT",
        price=2000.00,
        timestamp=datetime.now()
    )

    db_session.add(trade)
    db_session.commit()

