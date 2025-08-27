import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.agents import initialize_agent, Tool

from src.vr_automation import VRAutomation


class VRAutomationRunner:
    """Classe principal para execução do sistema de automação VR."""
    
    def __init__(self):
        """Inicializa o runner com configurações padrão."""
        self.setup_logging()
        self.load_environment()
        self.setup_paths()
        self.setup_llm()
        self.automation = VRAutomation()
        
    def setup_logging(self) -> None:
        """Configura o sistema de logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('vr_automation.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def load_environment(self) -> None:
        """Carrega variáveis de ambiente."""
        load_dotenv()
        
        self.openai_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_key:
            self.logger.error("❌ OPENAI_API_KEY não encontrada no arquivo .env")
            raise ValueError("Variável OPENAI_API_KEY é obrigatória")
            
        # Configurações do processamento
        self.month_competency = int(os.getenv("MONTH_COMPETENCY", "5"))
        self.year_competency = int(os.getenv("YEAR_COMPETENCY", "2025"))
        
        self.logger.info(f"✅ Configuração carregada: {self.month_competency:02d}/{self.year_competency}")
        
    def setup_paths(self) -> None:
        """Configura caminhos de arquivos e diretórios."""
        self.data_folder = Path("data")
        self.output_folder = Path("output")
        
        # Cria diretório de saída se não existir
        self.output_folder.mkdir(exist_ok=True)
        
        # Define nome do arquivo de saída
        self.output_file = self.output_folder / f"VR_MENSAL_{self.month_competency:02d}_{self.year_competency}.xlsx"
        
        # Verifica se pasta de dados existe
        if not self.data_folder.exists():
            self.logger.error(f"❌ Pasta de dados não encontrada: {self.data_folder}")
            raise FileNotFoundError(f"Diretório {self.data_folder} não existe")
            
        self.logger.info(f"📂 Dados: {self.data_folder} | Saída: {self.output_file}")
        
    def setup_llm(self) -> None:
        """Configura o modelo de linguagem."""
        try:
            self.llm = ChatOpenAI(
                model="gpt-4o-mini", 
                temperature=0,
                api_key=self.openai_key
            )
            self.logger.info("🤖 LLM configurado com sucesso")
        except Exception as e:
            self.logger.error(f"❌ Erro ao configurar LLM: {e}")
            raise
            
    def execute_complete_process(self, _: str = "") -> str:
        """Executa o processo completo de geração do arquivo consolidado."""
        try:
            self.logger.info("🚀 Iniciando processo completo...")
            
            result = self.automation.run_complete_process(
                files_folder=str(self.data_folder),
                month=self.month_competency,
                year=self.year_competency,
                output_file=str(self.output_file)
            )
            
            self.logger.info("✅ Processo completo executado com sucesso")
            return f"✅ Arquivo consolidado gerado: {self.output_file}\n{result}"
            
        except Exception as e:
            error_msg = f"❌ Erro no processo completo: {str(e)}"
            self.logger.error(error_msg)
            return error_msg
            
    def validate_data(self, _: str = "") -> str:
        """Valida os dados processados."""
        try:
            self.logger.info("🔍 Iniciando validação de dados...")
            
            validation_result = self.automation.validate_data()
            
            self.logger.info("✅ Validação concluída")
            return f"✅ Validação de dados concluída:\n{validation_result}"
            
        except Exception as e:
            error_msg = f"❌ Erro na validação: {str(e)}"
            self.logger.error(error_msg)
            return error_msg
            
    def generate_summary_report(self, _: str = "") -> str:
        """Gera relatório resumido por sindicato."""
        try:
            self.logger.info("📊 Gerando relatório resumido...")
            
            summary = self.automation.generate_summary_report()
            
            self.logger.info("✅ Relatório resumido gerado")
            return f"✅ Relatório resumido gerado:\n{str(summary)}"
            
        except Exception as e:
            error_msg = f"❌ Erro ao gerar relatório: {str(e)}"
            self.logger.error(error_msg)
            return error_msg
            
    def create_agent_tools(self) -> list:
        """Cria as ferramentas para o agente LLM."""
        return [
            Tool(
                name="Executar_Processo_Completo",
                func=self.execute_complete_process,
                description=f"Executa todo o processo de geração do arquivo consolidado de VR para {self.month_competency:02d}/{self.year_competency}. "
                           f"Processa todas as planilhas da pasta {self.data_folder} e gera o arquivo final."
            ),
            Tool(
                name="Validar_Dados",
                func=self.validate_data,
                description="Valida os dados processados para identificar inconsistências, "
                           "valores ausentes ou problemas de formatação nos dados consolidados."
            ),
            Tool(
                name="Gerar_Relatorio_Resumido",
                func=self.generate_summary_report,
                description="Gera um relatório resumido consolidado por sindicato, "
                           "apresentando estatísticas e totais por categoria."
            ),
        ]
        
    def create_agent(self):
        """Cria o agente de automação."""
        tools = self.create_agent_tools()
        
        return initialize_agent(
            tools=tools,
            llm=self.llm,
            agent="zero-shot-react-description",
            verbose=True,
            handle_parsing_errors=True,
            max_iterations=3
        )
        
    def run_automation_tasks(self) -> Dict[str, Any]:
        """Executa as tarefas principais de automação."""
        results = {}
        
        try:
            agent = self.create_agent()
            self.logger.info("🤖 Agente LLM iniciado com sucesso")
            
            # Tarefa 1: Processamento completo
            self.logger.info("=" * 60)
            self.logger.info("📋 TAREFA 1: Processamento Completo")
            self.logger.info("=" * 60)
            
            task1 = (f"Execute o processo completo para gerar o arquivo consolidado de VR "
                    f"a partir das planilhas da pasta {self.data_folder} "
                    f"para o período {self.month_competency:02d}/{self.year_competency}")
            
            result1 = agent.invoke({"input": task1})
            results['processamento_completo'] = result1
            self.logger.info("✅ Tarefa 1 concluída")
            
            # Tarefa 2: Relatório resumido
            self.logger.info("=" * 60)
            self.logger.info("📋 TAREFA 2: Relatório Resumido")
            self.logger.info("=" * 60)
            
            task2 = "Gere um relatório resumido consolidado por sindicato com as estatísticas principais"
            
            result2 = agent.invoke({"input": task2})
            results['relatorio_resumido'] = result2
            self.logger.info("✅ Tarefa 2 concluída")
            
            # Tarefa 3: Validação (opcional)
            self.logger.info("=" * 60)
            self.logger.info("📋 TAREFA 3: Validação dos Dados")
            self.logger.info("=" * 60)
            
            task3 = "Execute a validação dos dados processados para identificar possíveis inconsistências"
            
            result3 = agent.invoke({"input": task3})
            results['validacao'] = result3
            self.logger.info("✅ Tarefa 3 concluída")
            
        except Exception as e:
            error_msg = f"❌ Erro durante execução das tarefas: {str(e)}"
            self.logger.error(error_msg)
            results['error'] = error_msg
            
        return results
        
    def print_final_summary(self, results: Dict[str, Any]) -> None:
        """Imprime resumo final da execução."""
        print("\n" + "=" * 80)
        print("🎯 RESUMO FINAL DA EXECUÇÃO")
        print("=" * 80)
        
        if 'error' in results:
            print(f"❌ Execução finalizada com erros: {results['error']}")
            return
            
        print(f"📅 Período processado: {self.month_competency:02d}/{self.year_competency}")
        print(f"📂 Dados origem: {self.data_folder}")
        print(f"📄 Arquivo gerado: {self.output_file}")
        print(f"⏰ Processamento concluído em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        
        if self.output_file.exists():
            file_size = self.output_file.stat().st_size / 1024  # KB
            print(f"📊 Tamanho do arquivo: {file_size:.1f} KB")
            
        print("\n✅ Processo de automação VR finalizado com sucesso!")


def main():
    """Função principal de execução."""
    try:
        print("🚀 Iniciando Sistema de Automação VR")
        print("=" * 80)
        
        # Inicializa o runner
        runner = VRAutomationRunner()
        
        # Executa as tarefas
        results = runner.run_automation_tasks()
        
        # Imprime resumo final
        runner.print_final_summary(results)
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⚠️  Execução interrompida pelo usuário")
        return 1
        
    except Exception as e:
        print(f"\n❌ Erro crítico na execução: {str(e)}")
        logging.error(f"Erro crítico: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())