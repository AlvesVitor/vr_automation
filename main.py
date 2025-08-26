import sys
import os
from pathlib import Path

# Adicionar src ao path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.vr_automation import VRAutomation

def main():
    """Função principal de execução"""
    
    # Configurações
    DATA_FOLDER = "data"
    FOLDER_OUTPUT = "output" 
    MONTH_COMPETENCY = 5
    YEAR_COMPETENCY = 2025
    
    # Criar pasta de output se não existir
    os.makedirs(FOLDER_OUTPUT, exist_ok=True)
    
    # Nome do arquivo de saída
    output_file = os.path.join(FOLDER_OUTPUT, f'VR_MENSAL_{MONTH_COMPETENCY:02d}_{YEAR_COMPETENCY}.xlsx')
    
    print("🤖 AUTOMAÇÃO VR/VA")
    print("=" * 50)
    print(f"📁 Pasta de dados: {DATA_FOLDER}")
    print(f"📁 Pasta de saída: {FOLDER_OUTPUT}")
    print(f"📅 Competência: {MONTH_COMPETENCY:02d}/{YEAR_COMPETENCY}")
    print("=" * 50)
    
    try:
        # Verificar se pasta de dados existe
        if not os.path.exists(DATA_FOLDER):
            raise FileNotFoundError(f"Pasta '{DATA_FOLDER}' não encontrada!")
            
        # Executar automação
        automation = VRAutomation()
        
        generated_file = automation.run_complete_process(
            files_folder=DATA_FOLDER,
            month=MONTH_COMPETENCY,
            year=YEAR_COMPETENCY,
            output_file=output_file
        )
        
        print("\n" + "=" * 50)
        print("🎉 PROCESSAMENTO CONCLUÍDO!")
        print(f"📄 Arquivo gerado: {generated_file}")
        print("=" * 50)
        
        return generated_file
        
    except Exception as e:
        print(f"\n💥 ERRO FATAL: {e}")
        print(f"\n🔍 Tipo do erro: {type(e).__name__}")
        
        # Mostrar linha do erro se possível
        import traceback
        print(f"\n📍 Traceback completo:")
        traceback.print_exc()
        
        print(f"\n🔍 Verifique:")
        print("  • Se todos os arquivos estão na pasta 'data'")
        print("  • Se os arquivos não estão abertos no Excel")
        print("  • Se as permissões de escrita estão OK")
        print("  • Se os nomes dos arquivos estão exatos")
        print("  • Se as colunas dos arquivos estão corretas")
        return None

if __name__ == "__main__":
    main()