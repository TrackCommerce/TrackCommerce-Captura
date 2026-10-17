# TrackCommerce-Captura

Antes de iniciar a captura, o agente cadastra ou reativa automaticamente a
máquina na API TrackCommerce usando seu UUID e o token da empresa.

Crie um arquivo `.env` com base no `.env.example`, informe
`TOKEN_INSTALACAO` e `API_BASE_URL`, instale as dependências e execute:

```bash
pip install psutil python-dotenv boto3
python script-escrita.py
```

O agente interrompe a inicialização se o token estiver ausente, inválido ou se
a API não puder ativar a máquina. Reexecuções são idempotentes: o mesmo UUID é
reativado sem criar outro cadastro.
