from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel


class DbSettings(BaseModel):
    host: str
    port: int
    user: str
    password: str
    database: str


class YClientsSettings(BaseModel):
    api_base_url: str
    partner_token: str
    user_token: str
    company_id: int


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    # Telegram
    BOT_TOKEN: str

    # Ссылки для помощи и сообщества
    SUPPORT_CHAT_URL: str
    COMMUNITY_URL: str

    # YCLIENTS
    YCLIENTS_API_BASE_URL: str
    YCLIENTS_PARTNER_TOKEN: str
    YCLIENTS_USER_TOKEN: str
    YCLIENTS_COMPANY_ID: int

    # URLs
    BOOKING_URL: str
    BOOKING_PROXY_URL: str | None = None
    PRIVACY_POLICY_URL: str
    LEGAL_DOCS_URL: str

    # DB
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str = "db"
    POSTGRES_PORT: int = 5432

    # Misc
    LOG_LEVEL: str = "INFO"

    @property
    def db(self) -> DbSettings:
        return DbSettings(
            host=self.POSTGRES_HOST,
            port=self.POSTGRES_PORT,
            user=self.POSTGRES_USER,
            password=self.POSTGRES_PASSWORD,
            database=self.POSTGRES_DB,
        )

    @property
    def yclients(self) -> YClientsSettings:
        return YClientsSettings(
            api_base_url=self.YCLIENTS_API_BASE_URL.rstrip("/"),
            partner_token=self.YCLIENTS_PARTNER_TOKEN,
            user_token=self.YCLIENTS_USER_TOKEN,
            company_id=self.YCLIENTS_COMPANY_ID,
        )


settings = Settings()

