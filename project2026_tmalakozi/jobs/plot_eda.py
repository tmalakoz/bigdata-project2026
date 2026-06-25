import json
import matplotlib.pyplot as plt
import os

# Δημιουργία φακέλου για την αποθήκευση των PNG
os.makedirs('../results/plots', exist_ok=True)
json_path = '../results/metrics/eda_metrics.json'

print("Διαβάζω τα δεδομένα από το JSON...")
with open(json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# --- 1. Κατανομή Ωρών ---
hourly_data = data["hourly_distribution_2024"]
# Ταξινομούμε τις ώρες 0-23
hours = sorted([int(k) for k in hourly_data.keys()])
counts_hours = [hourly_data[str(h)] for h in hours]

plt.figure(figsize=(10, 6))
plt.bar(hours, counts_hours, color='skyblue', edgecolor='black')
plt.title('Κατανομή Διαδρομών ανά Ώρα (2024)')
plt.xlabel('Ώρα της Ημέρας (0-23)')
plt.ylabel('Πλήθος Διαδρομών')
plt.xticks(hours)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.savefig('../results/plots/1_hourly_distribution.png')
plt.close()

# --- 2. Κατανομή Ημερών ---
daily_data = data["daily_distribution_2024"]
days = sorted([int(k) for k in daily_data.keys()])
counts_days = [daily_data[str(d)] for d in days]

plt.figure(figsize=(12, 6))
plt.bar(days, counts_days, color='lightgreen', edgecolor='black')
plt.title('Κατανομή Διαδρομών ανά Ημέρα (2024)')
plt.xlabel('Ημέρα του Μήνα (1-31)')
plt.ylabel('Πλήθος Διαδρομών')
plt.xticks(days)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.savefig('../results/plots/2_daily_distribution.png')
plt.close()

# --- 3. Ιστόγραμμα Trip Distance (Λογαριθμική Κλίμακα) ---
dist_data = data["trip_distance_histogram_2024"]
distances = sorted([int(k) for k in dist_data.keys()])
counts_dist = [dist_data[str(d)] for d in distances]

plt.figure(figsize=(12, 6))
plt.bar(distances, counts_dist, color='coral', width=1.0, edgecolor='black')
plt.title('Ιστόγραμμα Απόστασης Διαδρομής (Λογαριθμική Κλίμακα)')
plt.xlabel('Απόσταση (Μίλια)')
plt.ylabel('Πλήθος Διαδρομών (Log Scale)')
plt.yscale('log') # Η εκφώνηση ζητάει λογαριθμική κλίμακα!
plt.xlim(0, max(distances))
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.savefig('../results/plots/3_trip_distance_histogram.png')
plt.close()

# --- 4. Ιστόγραμμα Total Amount (Λογαριθμική Κλίμακα) ---
amt_data = data["total_amount_histogram_2024"]
amounts = sorted([int(k) for k in amt_data.keys()])
counts_amt = [amt_data[str(a)] for a in amounts]

plt.figure(figsize=(12, 6))
plt.bar(amounts, counts_amt, color='purple', width=1.0)
plt.title('Ιστόγραμμα Συνολικού Κόστους (Λογαριθμική Κλίμακα)')
plt.xlabel('Συνολικό Κόστος ($)')
plt.ylabel('Πλήθος Διαδρομών (Log Scale)')
plt.yscale('log') # Και εδώ λογαριθμική!
plt.grid(axis='y', linestyle='--', alpha=0.7)
# Εμφάνιση αρνητικών και θετικών ορίων
plt.xlim(min(amounts), max(amounts)) 
plt.savefig('../results/plots/4_total_amount_histogram.png')
plt.close()

print("Τα γραφήματα δημιουργήθηκαν με επιτυχία στον φάκελο 'results/plots/'!")
