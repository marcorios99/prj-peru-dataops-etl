import pyodbc

# === Configura aquí ===
DRIVER   = "{ODBC Driver 17 for SQL Server}"  # o "{ODBC Driver 18 for SQL Server}"
SERVER   = r"PC-MARCO\SQLEXPRESS"             # alternativa: "PC-MARCO,1433" si fijaste puerto
DATABASE = "ETL_Conciliacion"

# Elige un modo de autenticación:
USE_WINDOWS_AUTH = True  # =False si usarás usuario/clave

USERNAME = "sa"          # solo si USE_WINDOWS_AUTH = False
PASSWORD = "TU_PASSWORD" # solo si USE_WINDOWS_AUTH = False

# Para Driver 18 suele requerirse TrustServerCertificate=yes o Encrypt=no
EXTRA_OPTS = "TrustServerCertificate=yes"     # para Driver 17/18; con 18 también puedes usar "Encrypt=no"

# ======================

if USE_WINDOWS_AUTH:
    conn_str = (
        f"DRIVER={DRIVER};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"Trusted_Connection=yes;"
        f"{EXTRA_OPTS};"
    )
else:
    conn_str = (
        f"DRIVER={DRIVER};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};PWD={PASSWORD};"
        f"{EXTRA_OPTS};"
    )

print("Connection string:", conn_str.replace(PASSWORD, "****") if not USE_WINDOWS_AUTH else conn_str)
print("Conectando...")

try:
    with pyodbc.connect(conn_str, timeout=5) as conn:
        cur = conn.cursor()
        cur.execute("SELECT @@SERVERNAME AS servername, DB_NAME() AS dbname, @@VERSION AS version;")
        row = cur.fetchone()
        print("\n✅ Conexión OK")
        print("Servidor:", row.servername)
        print("Base de datos:", row.dbname)
        print("Versión:\n", row.version)
except pyodbc.Error as e:
    print("\n❌ Error de conexión:")
    print(e)