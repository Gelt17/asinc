from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base
import logging
from config import Config

logger = logging.getLogger(__name__)


def create_database():
    try:
        engine = create_engine(Config.DATABASE_URL)
        with engine.connect():
            logger.info("✅ Подключение к базе данных успешно")
        Base.metadata.create_all(engine)
        return engine
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к базе данных: {e}")
        raise

def get_session():
    try:
        engine = create_engine(Config.DATABASE_URL)
        Session = sessionmaker(bind=engine)
        return Session()
    except Exception as e:
        logger.error(f"❌ Ошибка создания сессии: {e}")
        raise
