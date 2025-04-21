import pandas as pd
from abc import ABC, abstractmethod


class DataLoader(ABC):
    def __init__(self, params):
        self.params = params

    @abstractmethod
    def extract_data(self) -> pd.DataFrame:
        """Phương thức trừu tượng để các lớp con cài đặt"""
        pass

    def load_data(self, pd_data: pd.DataFrame, params: dict) -> bool:
        """Tuỳ từng lớp con override nếu cần"""
        return True
