import pytest
import os
import pandas as pd
from source.func import find_regression_coefficient, adjust_ethusdt_price, ethusdt_regression


@pytest.mark.asyncio
async def test_find_regression_coefficient():
    # Создаем тестовые данные
    df = pd.DataFrame({
        'Price_btc': [1, 2, 3, 4, 5],
        'Price_eth': [2, 4, 6, 8, 10]
    })
    # Вызываем тестируемую функцию
    coefficient = await find_regression_coefficient(df, 'Price_btc', 'Price_eth')
    # Проверяем, что коэффициент регрессии близок к ожидаемому значению
    # print (coefficient)
    assert pytest.approx(coefficient, abs=1e-6) == 2.0


@pytest.mark.asyncio
async def test_adjust_ethusdt_price():
    # Создаем тестовые данные
    data_frame = pd.DataFrame({
        'Price_eth': [100, 110, 120],
        'Price_btc': [50, 55, 60]
    })
    # Вызываем тестируемую функцию
    adjusted_prices = await adjust_ethusdt_price(data_frame)
    # Проверяем, что длина результирующего Series совпадает с ожидаемой
    assert len(adjusted_prices) == len(data_frame)
    # Проверяем, что скорректированные цены правильно рассчитаны
    assert adjusted_prices.tolist() == [-4.263256414560601e-14, -4.263256414560601e-14, -5.684341886080802e-14]


@pytest.mark.asyncio
async def test_ethusdt_regression():
    # Устанавливаем переменную окружения ENGINE
    os.environ['ENGINE'] = 'postgresql://postgres:1234@localhost:5432/postgres'
    # Создаем тестовые данные
    eth_df = pd.DataFrame({
        'Timestamp': ['2022-01-01 00:00:00', '2022-01-01 00:01:00'],
        'Price_eth': [1000, 1010]
    })
    btc_df = pd.DataFrame({
        'Timestamp': ['2022-01-01 00:00:00', '2022-01-01 00:01:00'],
        'Price_btc': [20000, 20100]
    })

    # Передаем тестовые данные в тестируемую функцию
    await ethusdt_regression(eth_df, btc_df)