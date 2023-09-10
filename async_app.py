import asyncio
from typing import List, Dict

import asyncpg
from asyncpg import Record
from loguru import logger
from tqdm import tqdm

import config as cfg


class RecSystem:
	def __init__(self):
		self.db: asyncpg.Pool = None
		self.five_minutes_points = 30  # m
		self.time_points = 100  # n

	async def create_connection(self):
		self.db: asyncpg.Pool = await asyncpg.create_pool(
			host=cfg.DB_HOST,
			port=cfg.DB_PORT,
			user=cfg.DB_USERNAME,
			password=cfg.DB_PASSWORD,
			database=cfg.DB_NAME
		)

	async def get_user_views(self, user_id: str, filter_zero_time_watch: bool = False) -> List[Record]:
		data = await self.db.fetch("SELECT * FROM player_starts_train WHERE user_id=$1", user_id)

		if filter_zero_time_watch:
			data = [i for i in data if i["watch_time"] > 0]  # Фильтруем записи (нулевое или отриц. время просмотра => удаляем)

		return data

	def get_count_views_for_every_viewed_videos(self, watching_data: list) -> Dict[int, int]:
		count_views = {}
		for data in watching_data:
			if data["item_id"] in count_views:
				count_views[data["item_id"]] += 1
			else:
				count_views[data["item_id"]] = 1

		return count_views

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

	def calc_recs_by_emotions(self, emotion_type):
		if emotion_type == "v_top":
			return self.time_points * 2
		elif emotion_type in ["pos_emotions", "neg_emotions"]:
			return self.time_points * 1.5

	async def get_video_info(self, video_id: str) -> Record:
		response = await self.db.fetchrow("SELECT * FROM videos WHERE item_id=$1", video_id)
		return response

	async def get_emotions_for_video(self, user_id: str, video_id: str) -> Record:
		response = await self.db.fetchrow("SELECT * FROM emotions WHERE user_id=$1 AND item_id=$2", user_id, video_id)
		return response

	async def get_videos_by_category(self, category):
		response = await self.db.fetch("SELECT item_id FROM videos WHERE category_title=$1", category)
		return response

	async def get_reccomendations(self, our_user_id: str):
		logger.warning("Загрузка пользователей...")
		all_users = await self.db.fetch("SELECT DISTINCT user_id FROM player_starts_train LIMIT 50")
		users_raiting = {user[0]: {"videos_raiting": {}, "categories_raiting": {}} for user in all_users}
		logger.success("Пользователи загружены. Структура создана!")

		for user_id in tqdm(users_raiting.keys(), desc="Users raiting create"):
			watching_data = await self.get_user_views(user_id, filter_zero_time_watch=True)
			count_views = self.get_count_views_for_every_viewed_videos(watching_data)

			##==> ЗАПОЛНЕНИЕ РЕЙТИНГА ВИДЕО
			#################################################################
			for view in watching_data:
				video_id = view["item_id"]
				video_info, emotions_for_video = await asyncio.gather(
					*[
						asyncio.create_task(self.get_video_info(video_id)),
						asyncio.create_task(self.get_emotions_for_video(user_id, video_id))
					]
				)

				##==> Добавление видео в рейтинг видео если его не было
				#################################################################
				if video_id not in users_raiting[user_id]["videos_raiting"]:
					users_raiting[user_id]["videos_raiting"].update({video_id: 0})

				##==> Добавляем баллы рекомендаций по удерживанию просмотра
				##################################################################
				duration = float(video_info[9] / 1000)
				watch_time = float(view["watch_time"])

				users_raiting[user_id]["videos_raiting"][video_id] += self.calc_recs_by_video_view_hold(
					duration,
					watch_time,
					self.five_minutes_points,
					self.time_points
				)

				##==> Добавление рейтинга за реакцию
				#########################################################
				if emotions_for_video is not None:
					users_raiting[user_id]["videos_raiting"][video_id] += self.calc_recs_by_emotions(emotions_for_video["type"])

				##==> Добавление рейтинга за просмотры
				#########################################################
				users_raiting[user_id]["videos_raiting"][video_id] *= count_views[video_id]

			##==> ЗАПОЛНЕНИЕ РЕЙТИНГА КАТЕГОРИЙ
			#################################################################
			for video_id in users_raiting[user_id]["videos_raiting"]:
				video_info = await self.get_video_info(video_id)
				category = video_info["category_title"]

				if category not in users_raiting[user_id]["categories_raiting"]:
					users_raiting[user_id]["categories_raiting"][category] = users_raiting[user_id]["videos_raiting"][
						video_id]
				else:
					users_raiting[user_id]["categories_raiting"][category] += users_raiting[user_id]["videos_raiting"][
						video_id]

		###############################################################################
		###############################################################################
		resp = await self.db.fetch("SELECT DISTINCT category_title FROM videos")
		all_categories = {}

		for category in resp:
			all_categories.update({category["category_title"]: {}})  # Добавляем category_id в all categories

			for user_id in users_raiting.keys():
				for video_id, raiting in users_raiting[user_id]["videos_raiting"].items():
					if video_id in all_categories[category["category_title"]]:
						all_categories[category["category_title"]][video_id] += raiting
					else:
						all_categories[category["category_title"]][video_id] = raiting
		###############################################################################
		###############################################################################
		sum_categories_raiting = 0
		for i in users_raiting[our_user_id]["categories_raiting"].values():
			sum_categories_raiting += i

		max_rec = 10
		count_vidios_in_recs = {
			category: raiting/sum_categories_raiting*max_rec
			for category, raiting in users_raiting[our_user_id]["categories_raiting"].items()
		}

		recommendations = []
		for category, count_videos in count_vidios_in_recs:
			sorted(all_categories[category].items(), key=lambda x: x[1])[::-1]









async def main():
	rec = RecSystem()
	await rec.create_connection()
	await rec.get_reccomendations("user_10002121")


if __name__ == "__main__":
	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())
