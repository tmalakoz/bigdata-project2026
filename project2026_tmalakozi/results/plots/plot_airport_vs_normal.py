import matplotlib.pyplot as plt
import numpy as np

# Κατηγορίες και Δεδομένα
metrics = ['Μέση Απόσταση', 'Μέση Διάρκεια (min)', 'Μέσο Κόστος ($)']
airport_data = [13.67, 26.58, 73.52]
normal_data = [7.16, 12.89, 22.82]

x = np.arange(len(metrics))
width = 0.35

plt.figure(figsize=(10, 6))

# Δημιουργία των στηλών
bars1 = plt.bar(x - width/2, airport_data, width, label='Διαδρομές Αεροδρομίου', color='royalblue', edgecolor='black')
bars2 = plt.bar(x + width/2, normal_data, width, label='Κανονικές Διαδρομές', color='lightgray', edgecolor='black')

# Ετικέτες και Τίτλος
plt.ylabel('Τιμές', fontsize=12)
plt.title('Σύγκριση Μετρικών: Αεροδρόμια vs Κανονικές Διαδρομές (Q5)', fontsize=14, pad=15)
plt.xticks(x, metrics, fontsize=12)
plt.legend(fontsize=11)
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Προσθήκη αριθμών πάνω από τις στήλες
for bar in bars1:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 1, f'{yval}', ha='center', fontweight='bold', color='darkblue')

for bar in bars2:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 1, f'{yval}', ha='center', fontweight='bold', color='dimgray')

plt.tight_layout()

# Αποθήκευση
plt.savefig('q5_airport_comparison.png', dpi=300)
plt.close()

print("Το γράφημα δημιουργήθηκε! Δες το q5_airport_comparison.png")
