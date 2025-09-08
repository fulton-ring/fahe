SELECT year, county,state_name, county_fips,
SUM("502_investment_dollars") as total_502_investment_dollars,
SUM("number_of_502_investment") as total_number_of_502_investment
FROM 'C:/Users/athar/OneDrive/Desktop/fulton_ring/fahe/final_data/*.csv'
GROUP BY year, county, state_name, county_fips
ORDER BY year, state_name, county
;