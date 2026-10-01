# Reglas del laboratorio src_dev

Versión política: **1.1.0**

1. Toda configuración sale de .env / ConfigSettings (producción).
2. Datos solo del exchange (REST público, WS, API privada con credenciales).
3. Credenciales obligatorias para ranking final (DEV_LAB_REQUIRE_CREDENTIALS=true).
4. Top N y notional desde env, no constantes en código.
5. Scoring derivado de métricas live + frecuencia real de órdenes en el exchange.
6. output/ guarda ranking completo + debug de conexiones + errores.

## Prohibido en ranking

- umbrales literales en código (ej. combined_buy=1.5 fijo)
- notional fijo 100 USDT sin CAPITAL_USDT
- top5 hardcodeado en nombre de archivo sin reflejar DEV_ORDER_LAB_TOP_N
- SQLite / JSONL / DuckDB locales como input del ranking
- pesos de score mágicos (0.85, 0.92) sin métrica live
- muestra de 50 órdenes sin paginar la API del exchange
- open_orders mezclados en estadísticas de tipos (solo order_history paginado)
