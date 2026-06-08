from datetime import datetime, timezone
from pydantic import BaseModel, Field

class WeatherData(BaseModel):
    city: str = Field(..., description="Nom de la ville")
    temperature: float = Field(..., description="Température en degrés Celsius")
    conditions: str = Field(..., description="Conditions météorologiques (ex: Sunny, Rainy)")
    humidity: int = Field(..., description="Pourcentage d'humidité", ge=0, le=100)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Date et heure de la mesure")
