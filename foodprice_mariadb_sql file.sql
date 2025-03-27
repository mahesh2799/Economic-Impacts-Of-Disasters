CREATE DATABASE IF NOT EXISTS FoodPricesDB;
USE FoodPricesDB;

CREATE TABLE FoodPrices (
    id INT PRIMARY KEY AUTO_INCREMENT,
    Country VARCHAR(100),
    Year INT,
    Month INT,
    FoodItem VARCHAR(100),
    UnitOfMeasurement VARCHAR(50),
    AveragePrice DECIMAL(10, 2),
    Currency VARCHAR(10),
    PriceInUSD DECIMAL(10, 2),
    Availability INT,
    Quality VARCHAR(50)
);
