package sativum

import peapod.Pea

/**
  * Created by marcin.mejran on 4/23/16.
  */
object AirflowGenerator {
  def generate(dag: Dag): String = {
    ""
  }

  private def dagSection(dag: Dag): String = {
    s"""dag = DAG(dag_id="${dag.name}")"""
  }

  //private def taskSection(task: Pea): String = {
//
  //}

  private def dagName(dag: Dag): String = {
    dag.name.replace("$","").replace(".","_")
  }

  private def taskName(pea: Pea[_]): String = {
    pea.toString.replace("$","").replace(".","_")
  }
}
