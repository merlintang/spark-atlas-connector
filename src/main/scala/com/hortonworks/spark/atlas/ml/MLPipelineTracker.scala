/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.atlas.ml

import java.lang.instrument.Instrumentation
import java.lang.instrument.ClassFileTransformer
import java.lang.management.ManagementFactory
import java.security.ProtectionDomain
import javassist.{ClassPool, CtClass}

import org.apache.spark.ml.Pipeline
import com.hortonworks.spark.atlas.utils.Logging
import com.sun.tools.attach.VirtualMachine

class AgentML extends Logging{

  var instrumentation:Instrumentation = null

  def premain(agentArgs:String, inst:Instrumentation) ={
    logInfo("premain invoked with args: {} and inst: {}", agentArgs, inst)
    instrumentation = inst
    assert(instrumentation != null)
    instrumentation.addTransformer(new HookerML())
  }

  def agentmain(agentArgs:String, inst:Instrumentation) : Unit ={
    logInfo("agentmain invoked with args: {} and inst: {}", agentArgs, inst)
    instrumentation = inst
  }

  def addHookerToInstrumentation(classname:String): Unit ={
    assert(instrumentation != null)
    instrumentation.addTransformer(new HookerML())
  }
}

object AgentML
{
  val jarFilePath = "/Users/mtang/Dev/spark/spark-atlas/" +
    "spark-atlas-connector/" +
    "target/spark-atlas-connector_2.11-0.1-SNAPSHOT-jar-with-dependencies.jar"

  // for dynamically loading agent
  def loadAgent(): Unit = {

    val nameOfRunningVM = ManagementFactory.getRuntimeMXBean().getName()
    val p = nameOfRunningVM.indexOf('@')
    val pid = nameOfRunningVM.substring(0, p)
    try {
      val vm = VirtualMachine.attach(pid)
      vm.loadAgent(jarFilePath, "")
      vm.detach()
    } catch {
      case ex: Exception => throw new Exception(ex)
    }
  }

}

class HookerML extends ClassFileTransformer with Logging {

  val ML_PIPELINE_FIT = "org.apache.spark.ml.pipeline"

  override def transform(loader: ClassLoader, className: String, classBeingRedefined: Class[_],
                         protectionDomain: ProtectionDomain, classfileBuffer: Array[Byte]): Array[Byte] = {

    if(className.replace("/", ".").equalsIgnoreCase(ML_PIPELINE_FIT)){
      logInfo("Instumenting " + ML_PIPELINE_FIT)

      try {
        val pool = ClassPool.getDefault
        var hookClass:CtClass = null
        try {
          hookClass = pool.get(ML_PIPELINE_FIT)
        }catch{
          case  nef: javassist.NotFoundException =>
            throw new Exception("javassist fail to get runtime classs" + ML_PIPELINE_FIT)
        }

        if(!hookClass.isInterface) {
          hookClass match {
            case o if o.isInstanceOf[Pipeline] =>
              val pipeline = o.asInstanceOf[Pipeline]
              logInfo("pipeline.uid" + pipeline.uid)

              val method_fit = hookClass.getDeclaredMethod("fit")
              val  pTypes = method_fit.getParameterTypes()
              pTypes.foreach {
                pType =>
                  if(pType.isPrimitive) {
                    logInfo("pType type" + pType)
                  }else {
                    logInfo("value" + pType)
                  }
              }
            case _ => logInfo("other class" + hookClass.toString)
          }
        }
      }catch {
        case ex: Exception =>
          throw new Exception("Agent fail to get runtime classs" + ML_PIPELINE_FIT)
      }
    }

    return classfileBuffer
  }

}

// Skeleton to track ML pipeline
class MLPipelineTracker extends Logging {



}
