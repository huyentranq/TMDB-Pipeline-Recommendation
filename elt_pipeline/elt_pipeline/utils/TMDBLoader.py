import requests
import pandas as pd
from typing import Dict
import os
from elt_pipeline.utils.data_loader import DataLoader

class TMDBLoader(DataLoader):
    def __init__(self, params: Dict):
        self.access_token = params.get("access_token")
        self.account_id = params.get("account_id")
        self.language = params.get("language", "en-US")
        self.sort_by = params.get("sort_by", "created_at.asc")
        self.page = params.get("page", 1)
        # Đổi backup_path về "/tmp/tmdb_backup.csv"
        self.backup_path = params.get("backup_path")

        self.fields_to_keep = [
            "adult", "genre_ids", "id", "overview", "popularity",
            "release_date", "title", "vote_average", "vote_count"
        ]

    def get_favorite_movies(self) -> pd.DataFrame:
        url = f"https://api.themoviedb.org/3/account/{self.account_id}/favorite/movies"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "accept": "application/json"
        }
        params = {
            "language": self.language,
            "page": self.page,
            "sort_by": self.sort_by
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])

            if not results:
                return pd.DataFrame()

            df = pd.DataFrame(results)

            # Lọc chỉ các trường cần giữ
            return df[self.fields_to_keep]
        else:
            raise Exception(f"Error fetching data: {response.status_code} - {response.text}")

    def extract_data(self) -> pd.DataFrame:
        new_df = self.get_favorite_movies()

        if new_df.empty:
            # Nếu không có phim mới => load từ backup cũ
            if os.path.exists(self.backup_path):
                return pd.read_csv(self.backup_path)
            else:
                return pd.DataFrame()

        # Nếu có phim mới => kiểm tra xem có phim nào chưa có trong backup
        if os.path.exists(self.backup_path):
            old_df = pd.read_csv(self.backup_path, converters={"genre_ids": self._safe_eval})
            new_ids = set(new_df["id"]) - set(old_df["id"])
            if not new_ids:
                return old_df
            # Lọc những phim mới được thêm vào
            only_new = new_df[new_df["id"].isin(new_ids)]
        else:
            # Nếu không có backup thì tất cả đều là mới
            only_new = new_df

        # Kiểm tra và tạo thư mục nếu cần
        os.makedirs(os.path.dirname(self.backup_path), exist_ok=True)
        
        # Lưu bản mới nhất (toàn bộ danh sách yêu thích)
        new_df.to_csv(self.backup_path, index=False)

        # Trả về chỉ những phim mới
        return only_new
    @staticmethod
    def _safe_eval(val):
        import ast
        try:
            result = ast.literal_eval(val)
            # Nếu không phải là list, trả về một list rỗng
            if not isinstance(result, list):
                return []
            return result
        except:
            return []
