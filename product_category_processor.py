from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ProductCategoryProcessor:
    """
    Класс для обработки данных о продуктах, категориях и связях между ними.

    Атрибуты:
        products (DataFrame): DataFrame, содержащий информацию о продуктах с колонками 'product_id' и 'product_name'.
        categories (DataFrame): DataFrame, содержащий информацию о категориях с колонками 'category_id' и 'category_name'.
        product_category (DataFrame): DataFrame, содержащий связи между продуктами и категориями с колонками 'product_id' и 'category_id'.
    """

    def __init__(self, products: DataFrame, categories: DataFrame, product_category: DataFrame):
        """
        Инициализирует экземпляр класса с заданными DataFrame.

        :param products: DataFrame с данными о продуктах.
        :param categories: DataFrame с данными о категориях.
        :param product_category: DataFrame со связями между продуктами и категориями.
        """
        self.products = products
        self.categories = categories
        self.product_category = product_category

    def get_product_category_pairs(self) -> DataFrame:
        """
        Формирует DataFrame с парами "Имя продукта – Имя категории". Также включает записи для продуктов,
        у которых нет категорий (для них значение в колонке 'category_name' будет null).

        Шаги:
            1. Выполняется соединение (inner join) таблицы product_category с таблицами products и categories,
               чтобы получить все пары связанных продуктов и категорий.
            2. Выполняется left-anti join для выбора продуктов, отсутствующих в таблице product_category.
            3. Результаты объединяются через union.

        :return: DataFrame с двумя колонками: 'product_name' и 'category_name'
        """
        # 1. Получаем пары "Имя продукта – Имя категории" через inner join связей с продуктами и категориями.
        df_pairs = self.product_category.join(
            self.products, self.product_category.product_id == self.products.product_id, "inner"
        ).join(
            self.categories, self.product_category.category_id == self.categories.category_id, "inner"
        ).select(
            self.products.product_name.alias("product_name"),
            self.categories.category_name.alias("category_name")
        )

        # 2. Выбираем продукты без категорий (left_anti join).
        df_no_category = self.products.join(
            self.product_category, self.products.product_id == self.product_category.product_id, "left_anti"
        ).select(
            self.products.product_name.alias("product_name")
        ).withColumn("category_name", F.lit(None))

        # 3. Объединяем результаты двух выборок.
        result_df = df_pairs.union(df_no_category)

        return result_df