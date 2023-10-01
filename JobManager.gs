/////////////////////////////////////////////////////////////////////////////////////////

class Job {

  constructor() {

    this.startTime = null
    this.MAX_EXECUTION_TIME = 300000
    this.shouldRetrigger = false
    this.scriptProperties = PropertiesService.getScriptProperties()
  
  }

  clear() {

    this.scriptProperties.setProperties({
      JobComplete: `false`,
      JobFunction: ``,
      JobProgress: ``,
      JobTrigger: ``,
      TotalRuntime: 0,
      NumberOfRuns: 0,
      AverageRuntime: 0
    })

    Logger.log(`Properties cleared.`)

    return this

  }

  start() {

    this.startTime = new Date().getTime()

    const properties = this.scriptProperties.getProperties()
    if (!(`JobComplete` in properties) || properties.JobComplete === `true`) this.clear()

    return this

  }

  setData(callback) {

    this.data = callback()

    return this

  }

  setFunction(callback) {

    this.func = callback

    return this
  
  }

  onCompletion(callback) {

    this.completionCallback = callback

    return this
  
  }

  retrigger(targetFunctionName = null) {

    if (this.shouldRetrigger) {

      const existingTrigger = this.scriptProperties.getProperty(`JobTrigger`)

      if (existingTrigger) {

        const trigger = ScriptApp
          .getProjectTriggers()
          .find(trigger => trigger.getUniqueId() === existingTrigger)
        
        ScriptApp.deleteTrigger(trigger)

      }
      
      const parentFunction = (targetFunctionName)
      ? targetFunctionName
      : new Error()
          .stack
          .split(`\n`)[3]
          .match(/at (.+) \(/)[1]

      const triggerId = ScriptApp
        .newTrigger(parentFunction)
        .timeBased()
        .at(new Date())
        .create()
        .getUniqueId()

      this.scriptProperties.setProperty(`JobTrigger`, triggerId)

    } else this.shouldRetrigger = true

    return this

  }

  run() {

    ///////////////////////////////////////////////////////////////////////////////////

    const formatTime = (ms) => {

      ms = Number(ms)
      const minutes = Math.floor(ms / 60000)
      const seconds = ((ms % 60000) / 1000).toFixed(0)
      
      return `${minutes}m ${seconds}s`

    }

    const estimateCompletion = (averageRuntime, currentIndex) => {

      const estimatedTimeInMS = (this.data.length - currentIndex) * averageRuntime

      Logger.log([
        `Estimated time remaining: ${formatTime(estimatedTimeInMS)}`,
        `Estimated completion: ${new Date(new Date().getTime() + estimatedTimeInMS)}`
      ].join(`\n`))

    }

    const cleanUp = () => {

      Logger.log([
        [`Runs: ${Number(this.scriptProperties.getProperty(`NumberOfRuns`)).toFixed(0)}`],
        [`Average Run: ${formatTime(this.scriptProperties.getProperty(`AverageRuntime`))}`],
        [`Total Runtime: ${formatTime(this.scriptProperties.getProperty(`TotalRuntime`))}`]        
      ].join(`\n`))

      const rerunTriggerId = this.scriptProperties.getProperty(`JobTrigger`)

      if (rerunTriggerId.length) {
        const trigger = ScriptApp
          .getProjectTriggers()
          .find(trigger => trigger.getUniqueId() === rerunTriggerId)
        ScriptApp.deleteTrigger(trigger)
      }

      [`JobComplete`, `JobFunction`, `JobProgress`, `JobTrigger`,
      `TotalRuntime`, `NumberOfRuns`, `AverageRuntime`]
      .forEach(property => this.scriptProperties.deleteProperty(property))

    }

    ///////////////////////////////////////////////////////////////////////////////////    

    const jobProgress = this.scriptProperties.getProperty(`JobProgress`).split(`*`) 

    let totalRuntime = parseInt(this.scriptProperties.getProperty(`TotalRuntime`))
    let numberOfRuns = parseInt(this.scriptProperties.getProperty(`NumberOfRuns`))
    const completedRuns = []

    for (const [index, item] of this.data.entries()) {

      const timeRemaining = this.MAX_EXECUTION_TIME - (new Date().getTime() - this.startTime)
      const averageRuntime = numberOfRuns > 0 ?  totalRuntime / numberOfRuns : 0

      if ((averageRuntime * 1.5) > timeRemaining) { 

        Logger.log(`Exceeding time limit, exiting.`)
        this.scriptProperties.setProperty(`JobProgress`, [...jobProgress, ...completedRuns].join(`*`))
        estimateCompletion(averageRuntime, index)
        if (this.shouldRetrigger) this.retrigger()
        return

      }

      if (jobProgress.includes(index.toString())) continue

      try {

        const runtimeStart = new Date().getTime()
        this.func(item)
        const runtimeEnd = new Date().getTime()

        completedRuns.push(index)

        totalRuntime += (runtimeEnd - runtimeStart)
        numberOfRuns++

        this.scriptProperties.setProperties({
          TotalRuntime: totalRuntime,
          NumberOfRuns: numberOfRuns,
          AverageRuntime: averageRuntime
        })

      } catch (error) {
        Logger.log(error)
      }

    }

    cleanUp()
    this.completionCallback()

  }

}

/////////////////////////////////////////////////////////////////////////////////////////
