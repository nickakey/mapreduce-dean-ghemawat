import * as fs from "fs";
import * as path from "path";

const mapBatchSize = 30; //todo make this part of class

class Task {
  state: "idle" | "in-progress" | "completed" = "idle"
  #assignedWorker: Machine | null = null;
  constructor(public type: "map" | "reduce" = "map", public path: string) { }
  assignWorker(worker: Machine) {
    this.#assignedWorker = worker
  }
  clearWorker() {
    this.#assignedWorker = null
  }
}

class MasterMachine {
  #workers: Machine[] = [];
  #timeout: NodeJS.Timeout | null = null;
  #tasks: Task[] = [];

  constructor(
    private mapCb: (value: string) => unknown,
    private reduceCb: (acc: any[], value: any) => any[],
    private filePath: string,
  ) { }

  #workerCount = 2; // todo make this dynamic based off of something
  init() {
    // create workers
    for (let i = 0; i < this.#workerCount; i++) {
      this.#workers.push(new Machine(this));
    }

    //Start the polling function that assigns idle tasks to idle workers
    this.#timeout = setInterval(() => {
      console.log('work assigner is running')
      const idleTasks = this.getIdleTasks();
      idleTasks.forEach((task) => {
        console.log('idle task found, attempting to assign ', task)
        const worker = this.getIdleWorker();
        if (worker) {
          console.log('idle worker found, assinging task')
          if (task.type === 'map') {

            //todo - you're going to need to like make these map tasks async so they actually work right
            worker.map(task, this.mapCb)
          } else if (task.type === 'reduce') {
            worker.reduce(task, this.reduceCb)
          }
        }
      })

      if (idleTasks.every(task => task.state === 'completed')) {
        this.#timeout?.close();
        console.log('Every task completed!!! We goated FR FR')
      }
    }, 5000)


    //Create some tasks for the workers to pick up
    this.#tasks.push(new Task("map", this.filePath))
  }

  getIdleTasks() {
    return this.#tasks.filter(t => t.state === "idle")
  }
  getIdleWorker() {
    console.log(this.#workers)
    return this.#workers.filter(rw => rw.state === "idle")[0]
  }
  receiveMappedDataPath(dataPath: string) {
    this.#tasks.push(new Task("reduce", dataPath))
  }
}

let iterationBlocker = 0;

class Machine {

  //todo I just realized that this state is actually on tasks themselves, NOT on the workers

  state: "idle" | "busy" = "idle";
  constructor(
    public masterMachine: MasterMachine,
  ) { }
  // naming this MAP is a bit confusing, I wish it could be like userMap or something idk
  map(task: Task, cb: (value: string) => unknown) {
    //delay this by a second to stimulate async 

    // todo also set the state of the tasks as we go through this
    setTimeout(() => {
      console.log('map task beginning')
      this.state = 'busy'
      task.state = 'in-progress'
      const absolutePath = path.resolve(task.path);
      const fileContent = fs.readFileSync(absolutePath, "utf-8");

      //for each row in filePath
      const rows = fileContent.split("\n"); //parse from file path

      //batch process the rows in case there are a ton
      for (let i = 0; i < rows.length; i += mapBatchSize) {
        if (iterationBlocker > 50) {
          return;
        }
        iterationBlocker += 1;
        //1
        const mappedRows = rows.slice(i, i + mapBatchSize).map(cb);

        //2
        // Periodically, the buffered pairs are written to local disk,
        // partitioned into R regions by the partitioning function.
        // The locations of these buffered pairs on the local disk are passed back to the master,
        // who is responsible for forwarding these locations to the reduce workers.
        const path = `mappedData/mappedRowsBatch-${i}.json`
        console.log({ mappedRows })
        fs.writeFile(
          path,
          JSON.stringify(mappedRows),
          (err) => {
            if (err) {
              console.error(err);
            } else {

              //3
              //notify the master of the locations of the rows we wrote
              console.log("Map task DONE! Notifying master!");
              this.masterMachine.receiveMappedDataPath(path)
            }
          },
        );
      }
      this.state = 'idle' //todo I'm not entirely sure what the point of completed is? Why not just back to idle?
      task.state = 'completed'
    }, 1000)
  }

  reduce(task: Task, cb: (acc: any[], el: any) => any[]) {
    setTimeout(() => {
      console.log('reduce is being called!')
      this.state = "busy";
      task.state = 'in-progress'

      const absolutePath = path.resolve(task.path);
      const fileContent = fs.readFileSync(absolutePath, "utf-8");

      //1
      //  When a reduce worker has read all intermediate data,
      //  it sorts it by the intermediate keys so that
      //  all occurrences of the same key are grouped together.
      //  The sorting is needed because typically many different keys
      //  map to the same reduce task.

      console.log({ fileContent })
      const kvPairs = JSON.parse(fileContent) as [string, any][][]
      const cleanedDataObj = kvPairs.flat().reduce<Record<string, any[]>>((acc, [k, v]) => {
        if (acc[k]) {
          acc[k].push(v)
        } else {
          acc[k] = [v]
        }
        return acc
      }, {})

      //todo I'm doing some crazy data maniupulation here...
      // like im mapping into object than iterating again to make an array?
      // this is lots of 0(n) methods back to back ... 
      const cleanedDataList = Object.entries(cleanedDataObj).map(([k, v]) => [k, v])

      //2
      //  If the amount of intermediate data is too large to fit in memory, an external sort is used.
      //The reduce worker iterates over the sorted intermediate data
      // and for each unique intermediate key encountered,
      // it passes the key and the corresponding set of intermediate values
      // to the user’s Reduce function.
      // The output of the Reduce function is appended to a final output file for this reduce partition.
      const reducedValues = cleanedDataList.reduce(cb);
      console.log('these are the reduced values!!!')
      console.log(reducedValues);
      fs.writeFile(
        `reducedData/reducedValues.json`,
        JSON.stringify(reducedValues),
        (err) => {
          if (err) {
            console.error(err);
          } else {

            //3
            //notify the master of the locations of the rows we wrote
            console.log("REDUCE task DONE!");
          }
        },
      );
      task.state = 'completed'
      this.state = 'idle'
    }, 1000)

  }
}

const main = (inputFilePath: string) => {
  //1: split the file into chunks (how many chunks?)

  //generate a kv pair for each word
  const mapFn = (s: string) =>
    s.trim().split(" ").map(word => [word.toLocaleLowerCase(), 1])

  //count the ocurrences per word and turn into object
  const reduceFn = (acc: [string, number][], [k, v]: [string, number[]]) => {
    //todo this spreading is costly I believe
    return [...acc, [k, v.reduce((acc, num) => acc + num, 0)]]
  }

  const masterMachine = new MasterMachine(
    mapFn,
    reduceFn,
    inputFilePath,
  );
  masterMachine.init();
};

main("names.txt");
