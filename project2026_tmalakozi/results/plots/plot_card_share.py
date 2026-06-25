import matplotlib.pyplot as plt

hours = ['07:00', '08:00', '09:00', '10:00']
card_share = [86.5, 87.0, 84.8, 83.3]

plt.figure(figsize=(8, 6))
# Φτιάχνουμε διάγραμμα γραμμής με τελείες (markers)
plt.plot(hours, card_share, marker='o', color='blue', linewidth=2, markersize=8)

plt.title('Ποσοστό Πληρωμών με Κάρτα ανά Ώρα (Q4)', fontsize=14, pad=15)
plt.xlabel('Ώρα (Πρωινό Παράθυρο)', fontsize=12)
plt.ylabel('Χρήση Κάρτας (%)', fontsize=12)
plt.ylim(80, 90) # Βάζουμε όρια στον άξονα Y για να φαίνεται καλύτερα η διαφορά
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Προσθήκη των ποσοστών πάνω στις τελείες
for i in range(len(hours)):
    plt.text(hours[i], card_share[i] + 0.3, f'{card_share[i]}%', ha='center', fontweight='bold')

plt.tight_layout()
plt.savefig('q4_card_share_trend.png', dpi=300)
plt.close()

print("Το γράφημα αποθηκεύτηκε ως q4_card_share_trend.png")
