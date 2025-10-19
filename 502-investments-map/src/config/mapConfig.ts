import { interpolateCool } from "d3-scale-chromatic";
import type { FieldConfig } from "@/components/FeatureInfoModal";

export const MapConfig = {
  // Map view settings
  centerLng: -81.5,
  centerLat: 37.5,
  zoom: 5,
  minZoom: 3,
  maxZoom: 20,

  // Basemap style
  mapStyle: "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",

  // Color scale
  colorScale: interpolateCool,

  // Investment per capita breakpoints
  investmentPerCapitaBreakpoints: [0, 32, 61, 77, 83, 108],

  // Income limit breakpoints (60% to 140% of income_limit_4_person)
  incomeLimitBreakpoints: [
    56832, 60413, 63025, 67453, 71640, 77474, 84469, 92247,
  ],

  // Available years
  availableYears: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023],

  // Layer styling
  fillOpacity: 0.5,
  fillOpacityHover: 0.4,
  lineOpacity: 0.8,
  lineColor: "#ffffff",
  lineWidth: 0.5,
  lineWidthHover: 2,

  // Data columns
  investmentColumn: "total_investment_dollars",
  populationColumn: "total_population",

  // Investment layer fields
  investmentFields: [
    { column: "year", label: "Year", format: (value: any) => String(value) },
    {
      column: "total_investment_dollars",
      label: "Total Investment",
      formatType: "currency",
    },
    {
      column: "total_number_of_investments",
      label: "Number of Investments",
      formatType: "number",
    },
    {
      column: "total_population",
      label: "Total Population",
      formatType: "number",
    },
    {
      column: "average_income_per_household",
      label: "Average Household Income",
      formatType: "currency",
    },
    {
      column: "total_median_earnings",
      label: "Median Earnings",
      formatType: "currency",
    },
    {
      column: "population_below_poverty",
      label: "Population Below Poverty",
      formatType: "number",
    },
    {
      column: "population_below_poverty_percent",
      label: "Poverty Rate",
      formatType: "percent",
    },
    {
      column: "number_of_households",
      label: "Number of Households",
      formatType: "number",
    },
    {
      column: "adults_25_and_older_less_than_high_school_graduate",
      label: "Adults Less Than High School",
      formatType: "number",
    },
    {
      column: "adults_25_and_older_less_than_high_school_graduate_percent",
      label: "Less Than High School Rate",
      formatType: "percent",
    },
    {
      column: "adults_25_and_older_with_bachelor's_degree_or_higher",
      label: "Adults with Bachelor's Degree+",
      formatType: "number",
    },
    {
      column: "adults_25_and_older_with_bachelor's_degree_or_higher_percent",
      label: "Bachelor's Degree+ Rate",
      formatType: "percent",
    },
  ] as FieldConfig[],

  // Income layer fields
  incomeFields: [
    { column: "year", label: "Year", format: (value: any) => String(value) },
    {
      column: "median_home_price",
      label: "Median Home Price",
      formatType: "currency",
    },
    {
      column: "income_limit_1_person",
      label: "Income Limit (1 person)",
      formatType: "currency",
    },
    {
      column: "income_limit_2_person",
      label: "Income Limit (2 people)",
      formatType: "currency",
    },
    {
      column: "income_limit_3_person",
      label: "Income Limit (3 people)",
      formatType: "currency",
    },
    {
      column: "income_limit_4_person",
      label: "Income Limit (4 people)",
      formatType: "currency",
    },
    {
      column: "income_limit_5_person",
      label: "Income Limit (5 people)",
      formatType: "currency",
    },
    {
      column: "income_limit_6_person",
      label: "Income Limit (6 people)",
      formatType: "currency",
    },
    {
      column: "income_limit_7_person",
      label: "Income Limit (7 people)",
      formatType: "currency",
    },
    {
      column: "income_limit_8_person",
      label: "Income Limit (8 people)",
      formatType: "currency",
    },
  ] as FieldConfig[],
};
