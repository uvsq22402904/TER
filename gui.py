import tkinter as tk
from tkinter import messagebox, font
from models.neo import neo
from models.m_sqlite import m_sqlite
import enviroment

BG_COLOR = "#f5f6fa"
FRAME_COLOR = "#dcdde1"
BTN_COLOR = "#487eb0"
BTN_TEXT_COLOR = "#fff"
LABEL_COLOR = "#353b48"
ENTRY_BG = "#fff"
ENTRY_FG = "#2d3436"
TITLE_COLOR = "#273c75"

def run_neo():
    update_env()
    try:
        neo(entry_db_test.get())
        messagebox.showinfo("Succès", "Transformation SQL → Neo4j terminée.")
    except Exception as e:
        messagebox.showerror("Erreur", f"Erreur lors de la transformation : {e}")

def run_m_sqlite():
    update_env()
    try:
        m_sqlite(entry_db_output.get())
        messagebox.showinfo("Succès", "Transformation Neo4j → SQL terminée.")
    except Exception as e:
        messagebox.showerror("Erreur", f"Erreur lors de la transformation : {e}")

def update_env():
    enviroment.URI_PORT_NEO4J = int(entry_port.get())
    enviroment.URI_AGENT_NEO4J = entry_agent.get()
    enviroment.URI_HOST_NEO4J = entry_host.get()
    enviroment.USERNAME_NEO4J = entry_user.get()
    enviroment.PASSWORD_NEO4J = entry_pass.get()
    enviroment.DATABASE_URI_TEST = entry_db_test.get()
    enviroment.DATABASE_URI_OUPUT_TEST = entry_db_output.get()
    enviroment.DATABASE_TYPE = entry_db_type.get()

def toggle_password():
    if entry_pass.cget('show') == '':
        entry_pass.config(show='*')
        btn_show_pass.config(text="Afficher")
    else:
        entry_pass.config(show='')
        btn_show_pass.config(text="Masquer")

root = tk.Tk()
root.title("Configuration & Transformation SQL ↔ Neo4j")
root.configure(bg=BG_COLOR)
root.geometry("900x520")
root.resizable(False, False)

default_font = font.nametofont("TkDefaultFont")
default_font.configure(family="Segoe UI", size=11)

# Titre
lbl_title = tk.Label(root, text="Transformation SQL ↔ Neo4j", font=("Segoe UI", 20, "bold"), fg=TITLE_COLOR, bg=BG_COLOR)
lbl_title.pack(pady=(18, 8))

# Neo4j config
frame_neo = tk.LabelFrame(root, text="Configuration Neo4j", padx=18, pady=12, bg=FRAME_COLOR, fg=LABEL_COLOR, font=("Segoe UI", 12, "bold"))
frame_neo.pack(fill="x", padx=30, pady=(0, 10))

tk.Label(frame_neo, text="Port :", bg=FRAME_COLOR, fg=LABEL_COLOR).grid(row=0, column=0, sticky="w", pady=2)
entry_port = tk.Entry(frame_neo, width=20, bg=ENTRY_BG, fg=ENTRY_FG)
entry_port.insert(0, enviroment.URI_PORT_NEO4J)
entry_port.grid(row=0, column=1, padx=(0, 15), pady=2)

tk.Label(frame_neo, text="Agent :", bg=FRAME_COLOR, fg=LABEL_COLOR).grid(row=0, column=2, sticky="w", pady=2)
entry_agent = tk.Entry(frame_neo, width=20, bg=ENTRY_BG, fg=ENTRY_FG)
entry_agent.insert(0, enviroment.URI_AGENT_NEO4J)
entry_agent.grid(row=0, column=3, padx=(0, 15), pady=2)

tk.Label(frame_neo, text="Host :", bg=FRAME_COLOR, fg=LABEL_COLOR).grid(row=1, column=0, sticky="w", pady=2)
entry_host = tk.Entry(frame_neo, width=20, bg=ENTRY_BG, fg=ENTRY_FG)
entry_host.insert(0, enviroment.URI_HOST_NEO4J)
entry_host.grid(row=1, column=1, padx=(0, 15), pady=2)

tk.Label(frame_neo, text="Utilisateur :", bg=FRAME_COLOR, fg=LABEL_COLOR).grid(row=1, column=2, sticky="w", pady=2)
entry_user = tk.Entry(frame_neo, width=20, bg=ENTRY_BG, fg=ENTRY_FG)
entry_user.insert(0, enviroment.USERNAME_NEO4J)
entry_user.grid(row=1, column=3, padx=(0, 15), pady=2)

tk.Label(frame_neo, text="Mot de passe :", bg=FRAME_COLOR, fg=LABEL_COLOR).grid(row=2, column=0, sticky="w", pady=2)
entry_pass = tk.Entry(frame_neo, width=20, show="*", bg=ENTRY_BG, fg=ENTRY_FG)
entry_pass.insert(0, enviroment.PASSWORD_NEO4J)
entry_pass.grid(row=2, column=1, padx=(0, 5), pady=2)
btn_show_pass = tk.Button(frame_neo, text="Afficher", command=toggle_password, width=9, bg="#b2bec3", fg="#222", relief="flat")
btn_show_pass.grid(row=2, column=2, padx=(0, 15), pady=2)

# SQL config
frame_sql = tk.LabelFrame(root, text="Configuration Base de données relationnelle", padx=18, pady=12, bg=FRAME_COLOR, fg=LABEL_COLOR, font=("Segoe UI", 12, "bold"))
frame_sql.pack(fill="x", padx=30, pady=(0, 10))

tk.Label(frame_sql, text="URI base SQL (pour neo) :", bg=FRAME_COLOR, fg=LABEL_COLOR).grid(row=0, column=0, sticky="w", pady=2)
entry_db_test = tk.Entry(frame_sql, width=50, bg=ENTRY_BG, fg=ENTRY_FG)
entry_db_test.insert(0, enviroment.DATABASE_URI_TEST)
entry_db_test.grid(row=0, column=1, padx=(0, 15), pady=2)

tk.Label(frame_sql, text="URI base SQL (pour m_sqlite) :", bg=FRAME_COLOR, fg=LABEL_COLOR).grid(row=1, column=0, sticky="w", pady=2)
entry_db_output = tk.Entry(frame_sql, width=50, bg=ENTRY_BG, fg=ENTRY_FG)
entry_db_output.insert(0, enviroment.DATABASE_URI_OUPUT_TEST)
entry_db_output.grid(row=1, column=1, padx=(0, 15), pady=2)

tk.Label(frame_sql, text="Type de base de données :", bg=FRAME_COLOR, fg=LABEL_COLOR).grid(row=2, column=0, sticky="w", pady=2)
entry_db_type = tk.Entry(frame_sql, width=20, bg=ENTRY_BG, fg=ENTRY_FG)
entry_db_type.insert(0, enviroment.DATABASE_TYPE)
entry_db_type.grid(row=2, column=1, padx=(0, 15), pady=2)

# Boutons d'action
frame_btn = tk.Frame(root, bg=BG_COLOR)
frame_btn.pack(pady=24)

btn_neo = tk.Button(
    frame_btn,
    text="Exécuter neo (SQL → Neo4j)",
    width=28,
    bg=BTN_COLOR,
    fg=BTN_TEXT_COLOR,
    font=("Segoe UI", 11, "bold"),
    relief="flat",
    command=run_neo,
    cursor="hand2",
    activebackground="#40739e"
)
btn_neo.grid(row=0, column=0, padx=24, pady=10)

btn_m_sqlite = tk.Button(
    frame_btn,
    text="Exécuter m_sqlite (Neo4j → SQL)",
    width=28,
    bg=BTN_COLOR,
    fg=BTN_TEXT_COLOR,
    font=("Segoe UI", 11, "bold"),
    relief="flat",
    command=run_m_sqlite,
    cursor="hand2",
    activebackground="#40739e"
)
btn_m_sqlite.grid(row=0, column=1, padx=24, pady=10)


root.mainloop()