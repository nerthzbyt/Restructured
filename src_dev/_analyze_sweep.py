import json
import os

p = os.path.join(os.path.dirname(__file__), "output", "full_system_sweep.json")
with open(p, encoding="utf-8") as f:
    d = json.load(f)

cur = "buy0.3_sl0.2_cb1.5_cs-1.5_hb0.5"
hits = [e for e in d["ranked_all"] if e["system_profile_id"] == cur]
hits.sort(key=lambda x: -x["composite_score"])
print(f"=== TOP 15 con .env actual ({cur}) ===")
for e in hits[:15]:
    print(
        f"#{e['rank']:>5} composite={e['composite_score']} lab={e['lab_score']} "
        f"{e['order_type']}+{e['time_in_force']} {e['side_hint']} "
        f"nertzh_fit={e['nertzh_production_fit']}"
    )

print("\n=== TOP 10 combos únicos (mejor perfil sistema cada uno) ===")
for e in d["top_recommendations"]["best_order_combo_unique"][:10]:
    print(f"{e['combo_id'][:60]}... composite={e['best_composite']} sys={e['best_system_profile']}")