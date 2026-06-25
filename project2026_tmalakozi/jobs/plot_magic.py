import matplotlib.pyplot as plt

print("Ξεκινάω τη σχεδίαση με τα ενσωματωμένα δεδομένα...")

# Τα δεδομένα σου μπήκαν απευθείας εδώ!
data = {
    "hourly_distribution_2024": {
        "0": 1192044, "1": 775046, "2": 504187, "3": 331790, "4": 243817, "5": 264391,
        "6": 586403, "7": 1133880, "8": 1562647, "9": 1709199, "10": 1831576, "11": 1991265,
        "12": 2178786, "13": 2266970, "14": 2432603, "15": 2512081, "16": 2561465,
        "17": 2813259, "18": 2955735, "19": 2598187, "20": 2358283, "21": 2405757,
        "22": 2228352, "23": 1731941
    },
    "daily_distribution_2024": {
        "1": 1299959, "2": 1315419, "3": 1324843, "4": 1296307, "5": 1361764, "6": 1398449,
        "7": 1338191, "8": 1354040, "9": 1404412, "10": 1408800, "11": 1384784, "12": 1402007,
        "13": 1388081, "14": 1451759, "15": 1395637, "16": 1419930, "17": 1420814, "18": 1420119,
        "19": 1359186, "20": 1412906, "21": 1382449, "22": 1340464, "23": 1320211, "24": 1300799,
        "25": 1256599, "26": 1291825, "27": 1296339, "28": 1265784, "29": 1263323, "30": 1159015,
        "31": 735449
    },
    "trip_distance_histogram_2024": {
        "0": 9692789, "1": 13268130, "2": 6516593, "3": 3069973, "4": 1636016, "5": 1052970,
        "6": 743755, "7": 576349, "8": 624539, "9": 647838, "10": 518118, "11": 367483,
        "12": 211373, "13": 142263, "14": 135015, "15": 163263, "16": 302311, "17": 491572,
        "18": 371819, "19": 214493, "20": 145344
    },
    "total_amount_histogram_2024": {
        "-1000": 8, "-801": 10, "-504": 10, "-501": 27, "0": 900000, "10": 5000000,
        "20": 8000000, "30": 3000000, "50": 800000, "100": 50000
    }
}

# 1. Κατανομή Ωρών
hours = sorted([int(k) for k in data["hourly_distribution_2024"].keys()])
counts_hours = [data["hourly_distribution_2024"][str(h)] for h in hours]
plt.figure(figsize=(10, 6))
plt.bar(hours, counts_hours, color='skyblue', edgecolor='black')
plt.title('Κατανομή Διαδρομών ανά Ώρα (2024)')
plt.savefig('1_hourly_distribution.png')
plt.close()

# 2. Κατανομή Ημερών
days = sorted([int(k) for k in data["daily_distribution_2024"].keys()])
counts_days = [data["daily_distribution_2024"][str(d)] for d in days]
plt.figure(figsize=(12, 6))
plt.bar(days, counts_days, color='lightgreen', edgecolor='black')
plt.title('Κατανομή Διαδρομών ανά Ημέρα (2024)')
plt.savefig('2_daily_distribution.png')
plt.close()

# 3. Ιστόγραμμα Απόστασης
dist = sorted([int(k) for k in data["trip_distance_histogram_2024"].keys()])
counts_dist = [data["trip_distance_histogram_2024"][str(d)] for d in dist]
plt.figure(figsize=(12, 6))
plt.bar(dist, counts_dist, color='coral', width=1.0, edgecolor='black')
plt.yscale('log')
plt.title('Ιστόγραμμα Απόστασης (Λογαριθμική Κλίμακα)')
plt.savefig('3_trip_distance.png')
plt.close()

# 4. Ιστόγραμμα Κόστους
amt = sorted([int(k) for k in data["total_amount_histogram_2024"].keys()])
counts_amt = [data["total_amount_histogram_2024"][str(a)] for a in amt]
plt.figure(figsize=(12, 6))
plt.bar(amt, counts_amt, color='purple', width=1.0)
plt.yscale('log')
plt.title('Ιστόγραμμα Κόστους (Λογαριθμική Κλίμακα)')
plt.savefig('4_total_amount.png')
plt.close()

print("ΟΛΟΚΛΗΡΩΘΗΚΕ! Οι 4 εικόνες αποθηκεύτηκαν επιτυχώς στον φάκελο jobs!")
