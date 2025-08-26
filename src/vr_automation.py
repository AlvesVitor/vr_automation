import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

class VRAutomation:
    def __init__(self):
        self.datas = {}
        self.final_result = None
        
    def load_datas(self, files_folder):
        """Carrega todas as bases de dados necessárias"""
        mapping_files = {
            'admissao': 'ADMISSÃO ABRIL.xlsx',
            'afastamentos': 'AFASTAMENTOS.xlsx', 
            'aprendiz': 'APRENDIZ.xlsx',
            'ativos': 'ATIVOS.xlsx',
            'dias_uteis': 'Base dias uteis.xlsx',
            'sindicato_valor': 'Base sindicato x valor.xlsx',
            'desligados': 'DESLIGADOS.xlsx',
            'estagio': 'ESTÁGIO.xlsx',
            'exterior': 'EXTERIOR.xlsx',
            'ferias': 'FÉRIAS.xlsx'
        }
        
        print("📁 Carregando bases de dados...")
        
        for name, file in mapping_files.items():
            path = os.path.join(files_folder, file)
            try:
                self.datas[name] = pd.read_excel(path)
                print(f"  ✅ {file}: {len(self.datas[name])} registros")
            except Exception as e:
                print(f"  ❌ Erro ao carregar {file}: {e}")
                
        return self.datas
    
    def clear_data(self):
        """Limpa e padroniza os dados das bases"""
        print("\n🧹 Limpando e padronizando dados...")
        
        # Padronizar colunas MATRICULA
        for name in ['admissao', 'afastamentos', 'aprendiz', 'ativos', 'desligados', 'estagio', 'ferias']:
            if name in self.datas and 'MATRICULA' in self.datas[name].columns:
                self.datas[name]['MATRICULA'] = self.datas[name]['MATRICULA'].astype(str).str.strip()
        
        # Padronizar coluna Cadastro para MATRICULA no arquivo exterior
        if 'exterior' in self.datas:
            self.datas['exterior']['MATRICULA'] = self.datas['exterior']['MATRICULA'].astype(str).str.strip()
            
        # Converter datas
        if 'admissao' in self.datas:
            self.datas['admissao']['Admissão'] = pd.to_datetime(self.datas['admissao']['Admissão'], errors='coerce')
            
        if 'desligados' in self.datas:
            self.datas['desligados']['DATA DEMISSÃO'] = pd.to_datetime(self.datas['desligados']['DATA DEMISSÃO'], errors='coerce')
            
        print("  ✅ Dados limpos e padronizados")
    
    def create_exclusion_base(self):
        """Cria lista consolidada de matrículas a serem excluídas"""
        print("\n🚫 Identificando exclusões...")
        
        exclusions = set()
        
        # Diretores (identificados pelo cargo)
        if 'ativos' in self.datas:
            directors = self.datas['ativos'][
                self.datas['ativos']['TITULO DO CARGO'].str.contains('DIRETOR', case=False, na=False)
            ]['MATRICULA'].tolist()
            exclusions.update(directors)
            print(f"  📊 Diretores: {len(directors)}")
        
        # Estagiários
        if 'estagio' in self.datas:
            interns = self.datas['estagio']['MATRICULA'].tolist()
            exclusions.update(interns)
            print(f"  📊 Estagiários: {len(interns)}")
            
        # Aprendizes
        if 'aprendiz' in self.datas:
            apprentices = self.datas['aprendiz']['MATRICULA'].tolist()
            exclusions.update(apprentices)
            print(f"  📊 Aprendizes: {len(apprentices)}")
            
        # Afastados
        if 'afastamentos' in self.datas:
            away = self.datas['afastamentos']['MATRICULA'].tolist()
            exclusions.update(away)
            print(f"  📊 Afastados: {len(away)}")
            
        # Exterior
        if 'exterior' in self.datas:
            exterior = self.datas['exterior']['MATRICULA'].tolist()
            exclusions.update(exterior)
            print(f"  📊 Exterior: {len(exterior)}")
        
        print(f"  🎯 Total de exclusões: {len(exclusions)}")
        return exclusions
    
    def create_consolidated_base(self, competence_month=5, competence_year=2025):
        """Cria a base consolidada final com todos os cálculos"""
        print(f"\n🔄 Criando base consolidada para {competence_month:02d}/{competence_year}...")
        
        # Base principal: Ativos
        if 'ativos' not in self.datas:
            raise ValueError("Base de ativos não encontrada!")
            
        base_final = self.datas['ativos'][['MATRICULA', 'TITULO DO CARGO', 'DESC. SITUACAO', 'SINDICATO']].copy()
        base_final['MATRICULA'] = base_final['MATRICULA'].astype(str)
        
        # Adicionar informações de admissão
        if 'admissao' in self.datas:
            admissions = self.datas['admissao'][['MATRICULA', 'Admissão']].copy()
            admissions['MATRICULA'] = admissions['MATRICULA'].astype(str)
            base_final = base_final.merge(admissions, on='MATRICULA', how='left')
        
        # Obter exclusões
        exclusions = self.create_exclusion_base()
        
        # Filtrar exclusões
        base_final = base_final[~base_final['MATRICULA'].isin(exclusions)]
        print(f"  📊 Após exclusões: {len(base_final)} colaboradores")
        
        # Adicionar informações de férias
        base_final['Dias_Ferias'] = 0
        if 'ferias' in self.datas:
            holidays_dict = dict(zip(
                self.datas['ferias']['MATRICULA'].astype(str), 
                self.datas['ferias']['DIAS DE FÉRIAS'].fillna(0)
            ))
            base_final['Dias_Ferias'] = base_final['MATRICULA'].map(holidays_dict).fillna(0)
        
        # Adicionar informações de desligamento
        base_final['Data_Demissao'] = None
        base_final['Comunicado_Desligamento'] = None
        base_final['Elegivel_Pagamento'] = True
        
        if 'desligados' in self.datas:
            disconnected_info = self.datas['desligados'].set_index('MATRICULA')
            
            for matricula in base_final['MATRICULA']:
                if matricula in disconnected_info.index:
                    dismissal_date = disconnected_info.loc[matricula, 'DATA DEMISSÃO']
                    statement = disconnected_info.loc[matricula, 'COMUNICADO DE DESLIGAMENTO']
                    
                    base_final.loc[base_final['MATRICULA'] == matricula, 'Data_Demissao'] = dismissal_date
                    base_final.loc[base_final['MATRICULA'] == matricula, 'Comunicado_Desligamento'] = statement
                    
                    # Regra de desligamento
                    if pd.notna(dismissal_date):
                        if statement == 'OK' and dismissal_date.day <= 15:
                            base_final.loc[base_final['MATRICULA'] == matricula, 'Elegivel_Pagamento'] = False
        
        # Filtrar apenas elegíveis para pagamento
        base_final = base_final[base_final['Elegivel_Pagamento'] == True]
        print(f"  📊 Elegíveis para pagamento: {len(base_final)} colaboradores")
        
        # Mapear dias úteis por sindicato
        business_days_dict = {}
        if 'dias_uteis' in self.datas:
            for _, row in self.datas['dias_uteis'].iterrows():
                sindicato = str(row['SINDICATO']).strip()
                days = row['DIAS UTEIS']
                business_days_dict[sindicato] = days
        
        # Mapear valores por estado/sindicato
        # Assumindo que o estado está no nome do sindicato
        state_values = {}
        if 'sindicato_valor' in self.datas:
            for _, row in self.datas['sindicato_valor'].iterrows():
                estado = row['ESTADO']
                value = float(str(row['VALOR']).replace('R$', '').replace(' ', '').replace(',', '.'))
                state_values[estado] = value
        
        # Calcular dias e valores
        results = []
        
        for _, employee in base_final.iterrows():
            registration = employee['MATRICULA']
            sindicato = employee['SINDICATO']
            admission_date = employee.get('Admissão')
            days_holidays = employee['Dias_Ferias']
            dismissal_date = employee['Data_Demissao']
            
            # Obter dias úteis do sindicato
            business_days_month = business_days_dict.get(sindicato, 22)  # Default 22 dias
            
            # Calcular dias trabalhados
            days_worked = business_days_month
            
            # Descontar férias
            days_worked -= days_holidays
            
            # Ajustar para admissões no meio do mês
            if pd.notna(admission_date):
                if admission_date.month == competence_month and admission_date.year == competence_year:
                    days_remaining_month = (30 - admission_date.day + 1)
                    proportion = days_remaining_month / 30
                    days_worked = int(business_days_month * proportion)
            
            # Ajustar para demissões proporcionais
            if pd.notna(dismissal_date):
                if dismissal_date.month == competence_month and dismissal_date.year == competence_year:
                    days_worked_mes = dismissal_date.day
                    proportion = days_worked_mes / 30
                    days_worked = int(business_days_month * proportion)
            
            # Garantir que dias não seja negativo
            days_worked = max(0, days_worked)
            
            # Determinar valor diário baseado no estado do sindicato
            daily_value = 35.0  # Default
            for estado, valor in state_values.items():
                if estado.upper() in sindicato.upper():
                    daily_value = valor
                    break
            
            # Calcular valores finais
            total_value = days_worked * daily_value
            company_cost = total_value * 0.8
            professional_discount = total_value * 0.2
            
            result = {
                'Matricula': registration,
                'Admissão': admission_date.strftime('%d/%m/%Y') if pd.notna(admission_date) else '',
                'Sindicato do Colaborador': sindicato,
                'Competência': f'01/{competence_month:02d}/{competence_year}',
                'Dias': days_worked,
                'VALOR DIÁRIO VR': daily_value,
                'TOTAL': total_value,
                'Custo empresa': company_cost,
                'Desconto profissional': professional_discount,
                'OBS GERAL': ''
            }
            
            results.append(result)
        
        self.final_result = pd.DataFrame(results)
        print(f"  ✅ Base consolidada criada: {len(self.final_result)} registros")
        
        return self.final_result
    
    def generate_final_report(self, output_file='VR_MENSAL_AUTOMATIZADO.xlsx'):
        """Gera o arquivo final no formato esperado"""
        if self.final_result is None:
            raise ValueError("Execute create_consolidated_base() primeiro!")
        
        print(f"\n📄 Gerando arquivo final: {output_file}")
        
        # Ordenar por matrícula
        self.final_result = self.final_result.sort_values('Matricula')
        
        # Salvar arquivo
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        self.final_result.to_excel(output_file, index=False)

        # Estatísticas
        total_collaborators = len(self.final_result)
        total_days = self.final_result['Dias'].sum()
        total_value = self.final_result['TOTAL'].sum()
        total_company_cost = self.final_result['Custo empresa'].sum()
        total_discount = self.final_result['Desconto profissional'].sum()
        
        print(f"  📊 Estatísticas:")
        print(f"     • Colaboradores: {total_collaborators}")
        print(f"     • Total de dias: {total_days}")
        print(f"     • Valor total: R$ {total_value:,.2f}")
        print(f"     • Custo empresa: R$ {total_company_cost:,.2f}")
        print(f"     • Desconto profissional: R$ {total_discount:,.2f}")
        print(f"  ✅ Arquivo salvo: {output_file}")
        
        return output_file
    
    def validate_data(self):
        """Executa validações nos dados processados"""
        print("\n🔍 Executando validações...")
        
        if self.final_result is None:
            print("  ❌ Nenhum dado para validar")
            return False
            
        validations = []
        
        # Validação 1: Dias não podem ser negativos
        negative_days = len(self.final_result[self.final_result['Dias'] < 0])
        if negative_days > 0:
            validations.append(f"❌ {negative_days} registros com dias negativos")
        else:
            validations.append("✅ Todos os registros têm dias válidos")
        
        # Validação 2: Valores não podem ser zero para dias > 0
        no_value = len(self.final_result[
            (self.final_result['Dias'] > 0) & (self.final_result['TOTAL'] == 0)
        ])
        if no_value > 0:
            validations.append(f"❌ {no_value} registros sem valor mas com dias > 0")
        else:
            validations.append("✅ Valores consistentes com dias trabalhados")
        
        # Validação 3: Conferir se custo empresa + desconto = total
        divergences = len(self.final_result[
            abs(self.final_result['Custo empresa'] + 
                self.final_result['Desconto profissional'] - 
                self.final_result['TOTAL']) > 0.01
        ])
        if divergences > 0:
            validations.append(f"❌ {divergences} registros com divergência nos valores")
        else:
            validations.append("✅ Rateio empresa/profissional correto")
        
        # Validação 4: Matrículas duplicates
        duplicates = len(self.final_result) - len(self.final_result['Matricula'].unique())
        if duplicates > 0:
            validations.append(f"❌ {duplicates} matrículas duplicates")
        else:
            validations.append("✅ Todas as matrículas são únicas")
        
        print("  " + "\n  ".join(validations))
        
        # Retornar True se todas as validações passaram
        return all("✅" in v for v in validations)
    
    def generate_summary_report(self):
        """Gera relatório resumido por sindicato"""
        if self.final_result is None:
            return None
            
        print("\n📈 Relatório por Sindicato:")
        
        summary = self.final_result.groupby('Sindicato do Colaborador').agg({
            'Matricula': 'count',
            'Dias': 'sum', 
            'TOTAL': 'sum',
            'Custo empresa': 'sum',
            'Desconto profissional': 'sum'
        }).round(2)
        
        summary.columns = ['Qtd_Funcionarios', 'Total_Dias', 'Valor_Total', 'Custo_Empresa', 'Desconto_Funcionario']
        
        for sindicato, dados in summary.iterrows():
            print(f"\n  📋 {sindicato[:50]}...")
            print(f"     • Funcionários: {dados['Qtd_Funcionarios']}")
            print(f"     • Dias: {dados['Total_Dias']}")
            print(f"     • Valor: R$ {dados['Valor_Total']:,.2f}")
            
        return summary

    def run_complete_process(self, files_folder, month=5, year=2025, output_file=None):
        """Executa todo o processo de automação"""
        print("🚀 Iniciando Automação VR/VA")
        print("="*50)
        
        try:
            self.load_datas(files_folder)
            
            self.clear_data()
            
            self.create_consolidated_base(month, year)
            
            if not self.validate_data():
                print("\n⚠️  Atenção: Algumas validações falharam!")
            
            self.generate_summary_report()
            
            if output_file is None:
                output_file = f'VR_MENSAL_{month:02d}_{year}.xlsx'
                
            self.generate_final_report(output_file)
            
            print("\n✅ Processo concluído com sucesso!")
            return output_file
            
        except Exception as e:
            print(f"\n❌ Erro durante o processo: {e}")
            raise
