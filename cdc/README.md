# **Documentação para replicação lógica em banco de dados PostgreSQL utilizando o conceito de *Data Change Capture (CDC)***

## **AWS Command Line Interface**
- [Instalar ou atualizar a versão mais recente da AWS CLI](https://docs.aws.amazon.com/pt_br/cli/latest/userguide/getting-started-install.html)
- [Configurações de arquivos de configuração e credenciais](https://docs.aws.amazon.com/pt_br/cli/latest/userguide/cli-configure-files.html)

## **Requisitos para implementação de replicações lógicas de banco de dados:**

- **Environment:** Amazon Web Service (AWS)
- **Database Engine:** Amazon RDS para PostgreSQL 14

## **Implementação de replicações lógicas de banco de dados:**

### **Passo 1:**

Criar uma nova instância de banco de dados para o novo projeto utilizando a linha de comando abaixo.

```bash
aws rds create-db-instance \
    --db-instance-identifier pgsql-db01-prod-master-pj01 \
    --engine postgres \
    --engine-version "14.2" \
    --db-instance-class db.t3.micro \
    --storage-type gp2 \
    --allocated-storage 20 \
    --max-allocated-storage 21 \
    --storage-encrypted \
    --port 5432 \
    --db-name db01_prod_pj01 \
    --master-username admin_user \
    --master-user-password "DbIg#*gOasX!" \
    --publicly-accessible \
    --no-multi-az \
    --backup-retention-period 1 \
    --tags Key="project",Value="01"
```

### **Passo 2:**

Criar uma replica de leitura da instância de banco de dados referênciada no **"Passo 1"** utilizando a linha de comando abaixo.

```bash
aws rds create-db-instance-read-replica \
    --db-instance-identifier pgsql-db01-prod-read-pj01 \
    --source-db-instance-identifier pgsql-db01-prod-master-pj01 \
    --db-instance-class db.t3.micro \
    --storage-type gp2 \
    --max-allocated-storage 21 \
    --port 5432 \
    --publicly-accessible \
    --no-multi-az \
    --tags Key="project",Value="01"
```

### **Passo 3:**

Criar um novo grupo de parâmetros para o novo projeto utilizando a linha de comando abaixo.

```bash
aws rds create-db-parameter-group \
    --db-parameter-group-name pgsql-cdc01-pj01 \
    --db-parameter-group-family postgres14 \
    --description "Parameter Group to Logical Replication - CDC" \
    --tags Key="project",Value="01"
```

Modificar o parâmetro *"rds.logical_replication"* do grupo de parâmetros referênciada no **"Passo 3"** utilizando a linha de comando abaixo.

```bash
aws rds modify-db-parameter-group \
    --db-parameter-group-name pgsql-cdc01-pj01 \
    --parameters "ParameterName=rds.logical_replication,ParameterValue=1,ApplyMethod=pending-reboot"
```

### **Passo 4:**

Habilitar a replicação lógica para instância de banco de dados referênciada no **"Passo 1"** utilizando a linha de comando abaixo. Após executar a linha de comando abaixo, será necessário que a instância de banco de dados referênciada no **"Passo 1"** seja reiniciada após a modificação ser aplicada.

```bash
aws rds modify-db-instance \
    --db-instance-identifier pgsql-db01-prod-master-pj01 \
    --db-parameter-group-name pgsql-cdc01-pj01
```

A replicação lógica utiliza o plugin [wal2json](https://github.com/eulerto/wal2json)

>IMPORTANTE:
>
>As tabelas que serão replicadas deverão ter obrigatóriamente *constraint* do tipo *primary key*.