"""
Тестовый скрипт для класса ProductCategoryProcessor.

Сценарий:
    1. Создаются тестовые DataFrame:
       - products: список продуктов с product_id и product_name;
       - categories: список категорий с category_id и category_name;
       - product_category: связи между продуктами и категориями.
    2. Инициализируется класс ProductCategoryProcessor с тестовыми данными.
    3. Вызывается метод get_product_category_pairs(), который возвращает DataFrame с парами
       «Имя продукта – Имя категории» и записями для продуктов без категорий.
    4. Результаты выводятся на экран и сравниваются с ожидаемыми.
"""

from pyspark.sql import SparkSession

# Импортируем класс для тестирования. Предполагается, что файл с классом называется
# product_category_processor.py и находится в той же директории, что и данный тестовый файл.
from product_category_processor import ProductCategoryProcessor


def create_test_data(spark):
    """
    Создаёт тестовые DataFrame для продуктов, категорий и связей между ними.

    :param spark: SparkSession для создания DataFrame.
    :return: Кортеж (products_df, categories_df, product_category_df)
    """
    # Тестовые данные для продуктов: (product_id, product_name)
    products_data = [
        (1, "Product A"),
        (2, "Product B"),
        (3, "Product C")  # Продукт без привязанных категорий
    ]

    # Тестовые данные для категорий: (category_id, category_name)
    categories_data = [
        (100, "Category X"),
        (101, "Category Y")
    ]

    # Тестовые данные для связей: (product_id, category_id)
    product_category_data = [
        (1, 100),
        (1, 101),
        (2, 100)  # Продукт B имеет одну категорию, а Product C не имеет привязок
    ]

    # Создаём DataFrame с явным указанием схемы.
    products_df = spark.createDataFrame(products_data, schema=["product_id", "product_name"])
    categories_df = spark.createDataFrame(categories_data, schema=["category_id", "category_name"])
    product_category_df = spark.createDataFrame(product_category_data, schema=["product_id", "category_id"])

    return products_df, categories_df, product_category_df


def main():
    """
    Основная функция тестирования.
    Создаёт SparkSession, тестовые данные, выполняет обработку данных через ProductCategoryProcessor,
    выводит результаты и проверяет их корректность.
    """
    # Инициализируем SparkSession
    spark = SparkSession.builder \
        .appName("ProductCategoryProcessorTest") \
        .master("local[*]") \
        .config("spark.driver.extraJavaOptions",
                "--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/javax.security.auth=ALL-UNNAMED") \
        .getOrCreate()

    # Создаём тестовые DataFrame
    products_df, categories_df, product_category_df = create_test_data(spark)

    # Инициализируем обработчик продуктов и категорий
    processor = ProductCategoryProcessor(products_df, categories_df, product_category_df)

    # Получаем итоговый DataFrame с парами «Имя продукта – Имя категории»
    result_df = processor.get_product_category_pairs()

    # Выводим результаты в консоль
    print("Результаты обработки:")
    result_df.show()

    # Для проверки собираем все строки DataFrame в список
    results = result_df.collect()

    # Ожидаемые результаты:
    # Для Product A должно быть 2 записи: ("Product A", "Category X") и ("Product A", "Category Y")
    # Для Product B - 1 запись: ("Product B", "Category X")
    # Для Product C - 1 запись: ("Product C", None)
    expected_results = [
        ("Product A", "Category X"),
        ("Product A", "Category Y"),
        ("Product B", "Category X"),
        ("Product C", None)
    ]

    # Преобразуем полученный результат в список кортежей для сравнения
    results_as_tuples = sorted([(row['product_name'], row['category_name']) for row in results])
    expected_sorted = sorted(expected_results)

    # Выполняем проверку: если результаты не соответствуют ожидаемым, выбрасываем исключение.
    assert results_as_tuples == expected_sorted, f"Ошибка теста: Ожидаемые результаты {expected_sorted}, а получено {results_as_tuples}"
    print("Все тесты прошли успешно!")

    # Останавливаем SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
