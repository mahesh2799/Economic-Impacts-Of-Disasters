LOAD DATA LOCAL INFILE 'C:/Users/HP/Downloads/Datasets/Final Datasets/Food Prices.csv'
INTO TABLE FoodPrices
FIELDS TERMINATED BY ','  -- Adjust if your delimiter is different
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES  -- Skip the header row
(Country, Year, Month, FoodItem, UnitOfMeasurement, AveragePrice, Currency, PriceInUSD, Availability, Quality);
