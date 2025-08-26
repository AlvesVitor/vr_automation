import os
from pathlib import Path
from functools import partial

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.agents import initialize_agent, Tool

from src.vr_automation import VRAutomation

load_dotenv()  # Carrega variáveis do .env

OPENAI_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_KEY:
    raise ValueError("❌ A variável OPENAI_API_KEY não foi encontrada. Defina no arquivo .env")

DATA_FOLDER = "data"
FOLDER_OUTPUT = "output"
MONTH_COMPETENCY = 5
YEAR_COMPETENCY = 2025

os.makedirs(FOLDER_OUTPUT, exist_ok=True)

OUTPUT_FILE = os.path.join(FOLDER_OUTPUT, f"VR_MENSAL_{MONTH_COMPETENCY:02d}_{YEAR_COMPETENCY}.xlsx")

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
automation = VRAutomation()


def execute_process(_: str):
    return automation.run_complete_process(
        files_folder=DATA_FOLDER,
        month=MONTH_COMPETENCY,
        year=YEAR_COMPETENCY,
        output_file=OUTPUT_FILE
    )

def dalidate_data(_: str):
    return automation.validate_data()

def generate_summary(_: str):
    return str(automation.generate_summary_report())

tools = [
    Tool(
        name="Executar Processo Completo",
        func=execute_process,
        description="Executa todo o processo de geração do arquivo consolidado de VR."
    ),
    Tool(
        name="Validar Dados",
        func=dalidate_data,
        description="Valida os dados processados para encontrar inconsistências."
    ),
    Tool(
        name="Gerar Relatório Resumido",
        func=generate_summary,
        description="Gera um resumo por sindicato após consolidar os dados."
    ),
]

agent = initialize_agent(
    tools=tools,
    llm=llm,
    agent="zero-shot-react-description",
    verbose=True
)

if __name__ == "__main__":
    print("🤖 Agente iniciado.")

    task1 = f"Gerar o arquivo consolidado de VR a partir das planilhas da pasta {DATA_FOLDER} para {MONTH_COMPETENCY}/{YEAR_COMPETENCY}"
    result1 = agent.invoke({"input": task1})
    print("📊 Resultado da Tarefa 1:", result1)

    task2 = "Criar um resumo consolidado por sindicato"
    result2 = agent.invoke({"input": task2})
    print("📑 Resultado da Tarefa 2:", result2)
