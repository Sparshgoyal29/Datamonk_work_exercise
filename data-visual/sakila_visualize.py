import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os

# === Setup ===
# Connect to database
conn = sqlite3.connect("sakila.db")

# Make folder for charts
os.makedirs("charts", exist_ok=True)

# Helper function to save + show plots
def save_show(name):
    plt.tight_layout()
    plt.savefig(f"charts/{name}.png")
    plt.show()

# ===========================================================
# 1. Monthly Rentals Over Time (Line Chart)
query = """
SELECT strftime('%Y-%m', rental_date) AS month, COUNT(*) AS rentals
FROM rental
GROUP BY month
ORDER BY month;
"""
df = pd.read_sql_query(query, conn)

plt.figure(figsize=(10,5))
plt.plot(df['month'], df['rentals'], marker='o', color='skyblue')
plt.title('Monthly Rentals Over Time')
plt.xlabel('Month')
plt.ylabel('Number of Rentals')
plt.xticks(rotation=45)
plt.grid(True)
save_show("monthly_rentals_over_time")

# ===========================================================
# 2. Top 10 Most Popular Films (Bar Chart)
query = """
SELECT f.title, COUNT(r.rental_id) AS rentals
FROM rental r
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
GROUP BY f.title
ORDER BY rentals DESC
LIMIT 10;
"""
df = pd.read_sql_query(query, conn)

plt.figure(figsize=(10,5))
plt.bar(df['title'], df['rentals'], color='orange')
plt.title("Top 10 Most Popular Films")
plt.xlabel("Film")
plt.ylabel("Number of Rentals")
plt.xticks(rotation=45, ha='right')
save_show("top10_popular_films")

# ===========================================================
# 3. Category Distribution (Pie Chart)
query = """
SELECT c.name AS category, COUNT(r.rental_id) AS rentals
FROM rental r
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
GROUP BY c.name;
"""
df = pd.read_sql_query(query, conn)

plt.figure(figsize=(8,8))
plt.pie(df['rentals'], labels=df['category'], autopct='%1.1f%%')
plt.title("Rental Distribution by Category")
save_show("category_distribution")

# ===========================================================
# 4. Revenue by Store (Bar Chart)
query = """
SELECT c.store_id, SUM(p.amount) AS revenue
FROM payment p
JOIN rental r ON p.rental_id = r.rental_id
JOIN customer c ON r.customer_id = c.customer_id
GROUP BY c.store_id;
"""
df = pd.read_sql_query(query, conn)

plt.figure(figsize=(6,5))
plt.bar(df['store_id'].astype(str), df['revenue'], color='green')
plt.title("Revenue by Store")
plt.xlabel("Store ID")
plt.ylabel("Revenue ($)")
save_show("revenue_by_store")

# ===========================================================
# 5. Rentals by Day of Week (Bar Chart)
query = """
SELECT strftime('%w', rental_date) AS weekday, COUNT(*) AS rentals
FROM rental
GROUP BY weekday
ORDER BY weekday;
"""
df = pd.read_sql_query(query, conn)
weekday_map = {
    "0": "Sunday", "1": "Monday", "2": "Tuesday",
    "3": "Wednesday", "4": "Thursday",
    "5": "Friday", "6": "Saturday"
}
df['weekday'] = df['weekday'].map(weekday_map)

plt.figure(figsize=(8,5))
plt.bar(df['weekday'], df['rentals'], color='purple')
plt.title("Rentals by Day of the Week")
plt.xlabel("Day of Week")
plt.ylabel("Number of Rentals")
save_show("rentals_by_day")

# ===========================================================
# 6. Top 5 Customers by Rentals (Bar Chart)
query = """
SELECT c.first_name || ' ' || c.last_name AS customer, COUNT(r.rental_id) AS rentals
FROM rental r
JOIN customer c ON r.customer_id = c.customer_id
GROUP BY customer
ORDER BY rentals DESC
LIMIT 5;
"""
df = pd.read_sql_query(query, conn)

plt.figure(figsize=(8,5))
plt.bar(df['customer'], df['rentals'], color='red')
plt.title("Top 5 Customers by Rentals")
plt.xlabel("Customer")
plt.ylabel("Number of Rentals")
save_show("top5_customers")

# ===========================================================
# 7. Average Rental Duration by Category (Bar Chart)
query = """
SELECT c.name AS category, AVG(f.rental_duration) AS avg_duration
FROM film f
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY avg_duration DESC;
"""
df = pd.read_sql_query(query, conn)

plt.figure(figsize=(10,5))
plt.bar(df['category'], df['avg_duration'], color='teal')
plt.title("Average Rental Duration by Category")
plt.xlabel("Category")
plt.ylabel("Avg Rental Duration (days)")
plt.xticks(rotation=45, ha='right')
save_show("avg_rental_duration_by_category")

# ===========================================================
# 8. Film Length vs Rental Count (Scatter Plot)
query = """
SELECT f.length, COUNT(r.rental_id) AS rentals
FROM film f
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY f.length;
"""
df = pd.read_sql_query(query, conn)

plt.figure(figsize=(8,5))
plt.scatter(df['length'], df['rentals'], alpha=0.6, color='brown')
plt.title("Film Length vs. Rental Count")
plt.xlabel("Film Length (minutes)")
plt.ylabel("Number of Rentals")
save_show("film_length_vs_rentals")

# ===========================================================
print("✅ All charts generated! Check the 'charts/' folder.")
