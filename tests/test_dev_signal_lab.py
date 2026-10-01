import os
import sys
import unittest

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE not in sys.path:
    sys.path.insert(0, BASE)

from src_dev.horizons.profiles import HorizonProfile, load_horizon_profiles
from src_dev.ml.dev_model import DevMLModel
from src_dev.observe.signal_observer import compare_with_live_bot


class DevSignalLabTests(unittest.TestCase):
    def test_horizon_profiles_from_env(self):
        profiles = load_horizon_profiles()
        self.assertGreater(len(profiles), 0)
        self.assertIsInstance(profiles[0], HorizonProfile)

    def test_compare_with_live_bot_aligned(self):
        prod_eval = {
            "signal": {"decision": "hold", "combined": 2.0, "blockers": ["combined_entre_umbrales"]},
            "prediction_level": {"level": "L2"},
        }
        live = {
            "prediction": {"decision": "hold", "level": "L2", "blockers": ["combined_entre_umbrales"]},
            "metrics": {"metrics": {"combined": 2.1, "metrics_calibrated": True}},
        }
        cmp = compare_with_live_bot(prod_eval, live)
        self.assertTrue(cmp["decision_match"])
        self.assertEqual(cmp["interpretation"], "alineado")

    def test_dev_ml_train_predict(self):
        model = DevMLModel()
        rows = []
        for i in range(12):
            rows.append(
                {
                    "production": {
                        "metrics": {
                            "combined": float(i - 6),
                            "combined_z": float(i - 6) / 10.0,
                            "pio": 0.1 * i,
                            "egm": 0.05 * i,
                            "ild": 0.0,
                            "rol": 0.0,
                            "ogm": 0.0,
                            "mom": 0.0,
                            "tfi": 0.0,
                            "volatility": 0.003,
                            "spread_bps": 0.02,
                            "rvol": 1e-6,
                        }
                    },
                    "horizons": {
                        "h1": {"combined": float(i - 6), "level": "L2", "decision": "hold"},
                    },
                    "forward_label": 1 if i >= 6 else 0,
                }
            )
        res = model.train_from_labeled_observations(rows, min_samples=8)
        self.assertTrue(res["success"])
        p = model.predict_proba(rows[-1])
        self.assertIsNotNone(p)
        self.assertGreaterEqual(p, 0.0)
        self.assertLessEqual(p, 1.0)


if __name__ == "__main__":
    unittest.main()