# Joining-Json-Files-in-Crunch
Program that takes inputs from multiple files and combines them, using Crunch on Hadoop

Here, I assume that there are four json files to be taken as input, each listing a product by its id, with one associated field, (brand, category, range and value) in the format on each line: {"product_id":"acceu34jmgv88y57", "category":"Accessories"}

I want to combine them, and make a json file as output, with each line having the following format: {"product_id":"acceu34jmgv88y57", "brand":"Best Brand", "category":"Accessories", "range":"this range", "value":"700"}
