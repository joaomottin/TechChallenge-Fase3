# Export de dados usado pela reprodução local

O arquivo `state_of_data_gold_export.csv` é um snapshot autorizado, em nível
de respondente, da base corrigida/preparada na execução principal da AWS. Na
AWS, os dados passaram pelas camadas Bronze, Silver e Gold; neste diretório
fica apenas o export usado para recalcular os indicadores Gold no VS Code.

Ele não é uma das tabelas Gold agregadas consultadas pelo Athena. O nome
`state_of_data_gold_export.csv` identifica o uso do arquivo na reprodução dos
indicadores e não significa que ele seja a saída agregada de uma única tabela
Gold.

O código procura por padrão esse arquivo neste diretório e não consulta S3,
Athena ou Glue durante a execução local. O snapshot representa o momento da
extração e não é uma leitura ao vivo do bucket. Para atualizar os gráficos,
substitua-o por um novo export autorizado da base preparada.

O CSV não é criado pelos scripts locais. Ele só deve ser distribuído ou
mantido no repositório quando houver autorização, sempre respeitando a
privacidade dos respondentes. Não coloque credenciais AWS, tokens ou chaves
neste arquivo ou no repositório.
