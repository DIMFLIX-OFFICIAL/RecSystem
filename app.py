from typing import Dict
import sqlite3


class RecSystem:
    def __init__(self, user_id):
        self.user_id = user_id
        self.db_connection = sqlite3.connect("General/data.db")

        self.pos_emotions = ["like", "heart", "happy_glasses", "new_fire", "happy_star_glasses", "laugh"]
        self.neg_emotions = ["dislike", "angry", "sickness", "gape", "closed_mouth", "crying"]

    def get_user_views(self, user_id, filter_zero_time_watch=False):
        data = self.db_connection.execute(
            "SELECT * FROM small_player_starts_train WHERE user_id=?", (user_id,)
        ).fetchall()

        if filter_zero_time_watch:
            data = [i for i in data if i[4] > 0]  # Фильтруем записи (нулевое или отриц. время просмотра => удаляем)

        return data

    def get_count_views_for_every_viewed_videos(self, watching_data: list) -> Dict[int, int]:
        count_views = {}
        for data in watching_data:
            if data[3] in count_views:
                count_views[data[3]] += 1
            else:
                count_views[data[3]] = 1

        return count_views

    def get_video_info(self, video_id: str) -> tuple:
        return self.db_connection.execute("SELECT * FROM videos WHERE item_id=?", (video_id,)).fetchone()

    def get_emotions_for_video(self, user_id: str, video_id: str):
        return self.db_connection.execute("SELECT * FROM emotions WHERE user_id=? AND item_id=?", (user_id, video_id)).fetchone()

    def calc_recs_by_video_view_hold(self, duration: float, watch_time: float, m: int, n: int):
        if duration < 300.0:
            if watch_time > 30.0:
                return m
            else:
                return 0

        elif 300.0 < duration < 2400.0:
            return watch_time / duration * n

        elif duration > 2400.0:
            return watch_time / 2400.0 * n

    def run_rec(self):
        all_users = self.db_connection.execute("SELECT DISTINCT user_id FROM small_player_starts_train;").fetchall()
        users_raiting = {user[0]: {"videos_raiting": {}, "categories_raiting": {}} for user in all_users}

        m = 30
        n = 100

        for user_id in users_raiting.keys():  # идем по юзерам
            watching_data = self.get_user_views(user_id, filter_zero_time_watch=True)  # информация о просмотрах
            count_views = self.get_count_views_for_every_viewed_videos(watching_data)  # количество просмотров

            for view in watching_data:
                video_id = view[3]
                video_info = self.get_video_info(video_id)
                emotions_for_video = self.get_emotions_for_video(user_id, video_id)

                ##==> Добавление видео в рейтинг видео если его не было
                #################################################################
                if video_id not in users_raiting[user_id]["videos_raiting"]:
                    users_raiting[user_id]["videos_raiting"].update({video_id: 0})

                ##==> Добавляем баллы рекомендаций по удерживанию просмотра
                ##################################################################
                duration = float(video_info[9] / 1000)
                watch_time = float(view[4])

                users_raiting[user_id]["videos_raiting"][video_id] += self.calc_recs_by_video_view_hold(
                    duration,
                    watch_time,
                    m,
                    n
                )

                ##==> Добавление рейтинга за реакцию
                #########################################################
                reaction_type = "none" if emotions_for_video is None or emotions_for_video[4] is None else emotions_for_video[4]
                if reaction_type == "v_top":
                    users_raiting[user_id]["videos_raiting"][video_id] += n * 2
                elif reaction_type == "pos_emotions" or reaction_type == "neg_emotions":
                    users_raiting[user_id]["videos_raiting"][video_id] += n * 1.5

                ##==> Добавление рейтинга за просмотры
                #########################################################
                users_raiting[user_id]["videos_raiting"][video_id] *= count_views[video_id]

            for video_id in users_raiting[user_id]["videos_raiting"]:
                video_info = self.get_video_info(video_id)
                category = video_info[7]

                if category not in users_raiting[user_id]["categories_raiting"]:
                    users_raiting[user_id]["categories_raiting"][category] = users_raiting[user_id]["videos_raiting"][video_id]
                else:
                    users_raiting[user_id]["categories_raiting"][category] += users_raiting[user_id]["videos_raiting"][video_id]

        print(users_raiting)


if __name__ == "__main__":
    rec = RecSystem("user_10002121")
    rec.run_rec()
