import * as fs from "fs";
import * as path from "path";

const mapBatchSize = 30; //todo make this part of class

class MasterMachine {
  #mapWorkers: Machine[] = [];
  #reduceWorkers: Machine[] = [];

  constructor(
    private mapCb: (value: string) => unknown,
    private reduceCb: (acc: any[], value: any) => any[],
    private filePath: string,
  ) {}

  #workerCount = 2; // todo make this dynamic based off of something
  init() {
    // create workers
    for (let i = 0; i < this.#workerCount / 2; i++) {
      this.#mapWorkers.push(new Machine(this, "map"));
      this.#reduceWorkers.push(new Machine(this, "reduce")); //todo - right now I'm just creating 2 reduce workers manually to handle the fact that map breaks up the input into 2 batches. But we really should actually build out the autoscaling mechanism or a work queue.
      this.#reduceWorkers.push(new Machine(this, "reduce"));
    }

    //assign map workers some work
    this.#mapWorkers.forEach((mw) => {
      mw.map(this.filePath, this.mapCb);
    });
  }

  getIdleReduceWorker(){
    console.log(this.#reduceWorkers)
    return this.#reduceWorkers.filter(rw => rw.state === "idle")[0]
  }
  receiveMappedDataPath(dataPath: string) {
    const idleReduceWorker = this.getIdleReduceWorker()
    if(!idleReduceWorker){
      console.log('no idle reduce worker found!')
      return 
    }
    //when I receive a path, assign it to an idle reduce worker I think?
    idleReduceWorker?.reduce(dataPath, this.reduceCb)
  }
}

let iterationBlocker = 0;

class Machine {
  state: "idle" | "in-progress" | "completed" = "idle";
  constructor(
    public masterMachine: MasterMachine,
    public type: "reduce" | "map",
  ) {}
  // naming this MAP is a bit confusing
  map(filePath: string, cb: (value: string) => unknown) {
    console.log('map task beginning')
    this.state = 'in-progress'
    const absolutePath = path.resolve(filePath);
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
    this.state = 'completed' //todo I'm not entirely sure what the point of completed is? Why not just back to idle?
  }

  // todo - type this cb
  reduce(mappedFilePath: string, cb: (acc: any[], el: any) => any[]) {
    console.log('reduce is being called!')
    this.state = "in-progress";

    const absolutePath = path.resolve(mappedFilePath);
    const fileContent = fs.readFileSync(absolutePath, "utf-8");
    
    //1
    //  When a reduce worker has read all intermediate data,
    //  it sorts it by the intermediate keys so that
    //  all occurrences of the same key are grouped together.
    //  The sorting is needed because typically many different keys
    //  map to the same reduce task.

    const kvPairs = JSON.parse(fileContent) as [string, any][][]
    const cleanedDataObj = kvPairs.flat().reduce<Record<string, any[]>>((acc, [k, v]) => {
      if(acc[k]){
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
    console.log(reducedValues)
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
    return [...acc, [k, v.reduce((acc, num)=>acc+num, 0)]]
  }

  const masterMachine = new MasterMachine(
    mapFn,
    reduceFn,
    inputFilePath,
  );
  masterMachine.init();
};

main("names.txt");
