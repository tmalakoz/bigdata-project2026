import matplotlib.pyplot as plt

# Τα δεδομένα σου
zones = [
    "JFK Airport", "LaGuardia Airport", "Two Bridges/Seward Park",
    "Chinatown", "Financial District South", "Alphabet City",
    "Financial District North", "Seaport", "Lower East Side", "Midtown Center"
]
revenue = [
    346.25, 191.14, 27.76, 23.95, 23.27, 22.65, 22.65, 22.06, 22.00, 21.77
]

# Αντιστροφή της λίστας για να εμφανιστεί το JFK στην κορυφή του γραφήματος
zones = zones[::-1]
revenue = revenue[::-1]

plt.figure(figsize=(12, 8))
bars = plt.barh(zones, revenue, color='teal', edgecolor='black')

# Ετικέτες και τίτλος
plt.xlabel('Έσοδα ανά Μίλι ($/mile)', fontsize=12)
plt.ylabel('Περιοχή Επιβίβασης (Zone)', fontsize=12)
plt.title('Top 10 Περιοχές με τα Υψηλότερα Έσοδα ανά Μίλι (Q5)', fontsize=14, pad=15)
plt.grid(axis='x', linestyle='--', alpha=0.7)

# Προσθήκη των ακριβών ποσών δίπλα από κάθε μπάρα για πιο επαγγελματικό αποτέλεσμα
for bar in bars:
    plt.text(bar.get_width() + 5, bar.get_y() + bar.get_height()/2,
             f'${bar.get_width():.2f}',
             va='center', ha='left', fontsize=11, fontweight='bold')

# Αυτόματη προσαρμογή περιθωρίων ώστε να μην κοπούν τα μεγάλα ονόματα
plt.tight_layout()

# Αποθήκευση σε PNG με υψηλή ανάλυση (300 dpi)
plt.savefig('q5_revenue_per_mile.png', dpi=300)
plt.close()

print("Το γράφημα δημιουργήθηκε με επιτυχία! Δες το αρχείο q5_revenue_per_mile.png")
