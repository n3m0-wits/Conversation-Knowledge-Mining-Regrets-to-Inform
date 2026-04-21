from typing import List

from pydantic import BaseModel, Field


class SelectedFilters(BaseModel):
    Company: List[str] = Field(default_factory=list)
    Portal: List[str] = Field(default_factory=list)
    Category: List[str] = Field(default_factory=list)
    Urgency: List[str] = Field(default_factory=list)
    ActionRequired: List[str] = Field(default_factory=list)
    DateRange: List[str] = Field(default_factory=list)


class ChartFilters(BaseModel):
    selected_filters: SelectedFilters
