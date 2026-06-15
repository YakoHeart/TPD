#!/usr/bin/env python3
"""
Contagem de palavras em múltiplos arquivos usando paralelismo (multiprocessing)
com tratamento de falhas (retentativas) e padrão MapReduce.
"""

import os
import sys
import time
import logging
from multiprocessing import Pool
from functools import partial

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def count_words_in_file(filename):
    """Conta palavras em um único arquivo. Retorna dicionário {palavra: contagem}."""
    word_count = {}
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                for word in line.lower().split():
                    word = word.strip('.,!?;:()[]{}"\'')
                    if word:
                        word_count[word] = word_count.get(word, 0) + 1
        return word_count
    except Exception as e:
        raise RuntimeError(f"Erro ao processar {filename}: {str(e)}")

def process_with_retry(filename, max_retries=2):
    """Tenta processar um arquivo com retentativas."""
    for attempt in range(max_retries):
        try:
            result = count_words_in_file(filename)
            return (filename, result, None)
        except Exception as e:
            logging.warning(f"Falha ao processar {filename} (tentativa {attempt+1}): {e}")
            time.sleep(0.5)
    return (filename, None, "Todas as tentativas falharam")

# Função auxiliar para ser usada no pool (recebe um único argumento)
# Ela desempacota o nome do arquivo e o número de retentativas
def worker_process(args):
    filename, max_retries = args
    return process_with_retry(filename, max_retries)

def main(file_list, num_workers=None, max_retries=2):
    if not file_list:
        logging.error("Nenhum arquivo fornecido.")
        return

    if num_workers is None:
        num_workers = os.cpu_count() or 1
    logging.info(f"Processando {len(file_list)} arquivos com {num_workers} workers")

    # Prepara lista de argumentos: cada elemento é (filename, max_retries)
    args_list = [(f, max_retries) for f in file_list]

    with Pool(num_workers) as pool:
        resultados = pool.map(worker_process, args_list)

    # Combina os resultados
    final_counts = {}
    failures = []
    for filename, counts, error in resultados:
        if counts is None:
            failures.append((filename, error))
        else:
            for word, cnt in counts.items():
                final_counts[word] = final_counts.get(word, 0) + cnt

    if failures:
        logging.warning(f"\n{len(failures)} arquivo(s) falharam:")
        for fname, err in failures:
            logging.warning(f"  - {fname}: {err}")
    else:
        logging.info("Todos os arquivos processados com sucesso.")

    sorted_words = sorted(final_counts.items(), key=lambda x: x[1], reverse=True)
    print("\n=== RESULTADO (20 palavras mais frequentes) ===")
    for word, count in sorted_words[:20]:
        print(f"{word}: {count}")
    print(f"\nTotal de palavras distintas: {len(final_counts)}")

# Demonstração de seção crítica com lock (exigência do trabalho)
def exemploSecaoCritica():
    from multiprocessing import Process, Manager
    manager = Manager()
    contador = manager.dict()
    lock = manager.Lock()

    def worker(lock, contador):
        for _ in range(1000):
            with lock:
                contador['total'] = contador.get('total', 0) + 1

    procs = [Process(target=worker, args=(lock, contador)) for _ in range(4)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    print(f"Contador final (deveria 4000): {contador.get('total', 0)}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python wordcount.py arquivo1.txt arquivo2.txt ...")
        sys.exit(1)
    main(sys.argv[1:], num_workers=4, max_retries=2)
    # exemploSecaoCritica()   # Descomente para testar a seção crítica