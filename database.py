from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from models import Base
import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

class AsyncDatabase:
    def __init__(self):
        self.engine = None
        self.async_session = None

    async def init(self):
        # Получаем DATABASE_URL из переменных окружения
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL не найден в переменных окружения")
        
        # Заменяем postgresql:// на postgresql+asyncpg:// для асинхронного подключения
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://")
        self.engine = create_async_engine(database_url, echo=False)
        self.async_session = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )
        
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    def get_session(self):
        return self.async_session()