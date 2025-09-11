# part1_charts.py
import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# create output dir
out_dir = "plots"
os.makedirs(out_dir, exist_ok=True)

sns.set(style="whitegrid")

# --- 1) Histogram of Tip Amounts ---
tips = sns.load_dataset("tips")
plt.figure(figsize=(8,5))
sns.histplot(tips["tip"], bins=20, kde=True)
plt.title("Distribution of Tips")
plt.xlabel("Tip ($)")
plt.ylabel("Count")
plt.tight_layout()
plt.savefig(os.path.join(out_dir, "tips_histogram.png"))
plt.close()

# --- 2) Compare Total Bills by Time of Day (Lunch vs Dinner) ---
# We'll show both SUM and MEAN -> two visual small charts
bill_by_time_sum = tips.groupby("time", as_index=False)["total_bill"].sum().rename(columns={"total_bill": "total_bill_sum"})
bill_by_time_mean = tips.groupby("time", as_index=False)["total_bill"].mean().rename(columns={"total_bill": "total_bill_mean"})

plt.figure(figsize=(6,4))
sns.barplot(data=bill_by_time_sum, x="time", y="total_bill_sum")
plt.title("Total Bill (SUM) by Time of Day")
plt.ylabel("Total Bill (sum)")
plt.tight_layout()
plt.savefig(os.path.join(out_dir, "total_bill_by_time_sum.png"))
plt.close()

plt.figure(figsize=(6,4))
sns.barplot(data=bill_by_time_mean, x="time", y="total_bill_mean")
plt.title("Total Bill (MEAN) by Time of Day")
plt.ylabel("Average Total Bill ($)")
plt.tight_layout()
plt.savefig(os.path.join(out_dir, "total_bill_by_time_mean.png"))
plt.close()

# --- 3) Box Plot of Total Bill by Day ---
plt.figure(figsize=(8,5))
sns.boxplot(data=tips, x="day", y="total_bill")
plt.title("Total Bill by Day (Boxplot)")
plt.ylabel("Total Bill ($)")
plt.tight_layout()
plt.savefig(os.path.join(out_dir, "total_bill_by_day_boxplot.png"))
plt.close()

# --- 4) Heatmap (flights dataset) ---
flights = sns.load_dataset("flights")
pivot = flights.pivot(index="month", columns="year", values="passengers")
plt.figure(figsize=(12,6))
sns.heatmap(pivot, cmap="YlGnBu", annot=False)  # annot=True can be noisy, but try if you want numbers
plt.title("Flights: Passengers by Month & Year (Heatmap)")
plt.tight_layout()
plt.savefig(os.path.join(out_dir, "flights_heatmap.png"))
plt.close()

print("All plots saved to:", out_dir)
