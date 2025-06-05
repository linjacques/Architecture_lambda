import time
import shutil
import os

def compact_files(spark, input_dir, output_dir, repartition_num=1):
    df = spark.read.option("mergeSchema", "true").json(input_dir)

    df = df.repartition(repartition_num)

    temp_output = f"{output_dir}_tmp"
    df.write.mode("overwrite").option("header", True).json(temp_output)

    # Remplacer l'ancien dossier (facultatif : sauvegarder avant)
    shutil.rmtree(output_dir, ignore_errors=True)
    shutil.move(temp_output, output_dir)
