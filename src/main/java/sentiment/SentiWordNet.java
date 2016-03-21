package sentiment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

/**
 * Copied from http://sentiwordnet.isti.cnr.it/code/SWN3.java
 * extract() method takes a word, gives a score for the word.
 * 1 means strongly positive, 0 means neutral, -1 means strongly negative.
 */
public class SentiWordNet {
  private URL pathToSWN = this.getClass().getClassLoader().getResource("SentiWordNet_3.0.0.txt");
  private HashMap<String, Double> _dict;

  //Initialize a dictionary that maps a word to its score
  public SentiWordNet() {
    _dict = new HashMap<>();
    HashMap<String, Vector<Double>> _temp = new HashMap<>();
    try{
      BufferedReader csv =  new BufferedReader(new FileReader(Paths.get(pathToSWN.toURI()).toFile()));
      String line = "";
      while((line = csv.readLine()) != null) {
        String[] data = line.split("\t");
        Double score = Double.parseDouble(data[2])-Double.parseDouble(data[3]);
        String[] words = data[4].split(" ");
        for(String w:words) {
          String[] w_n = w.split("#");
          w_n[0] += "#"+data[0];
          int index = Integer.parseInt(w_n[1])-1;
          if(_temp.containsKey(w_n[0])) {
            Vector<Double> v = _temp.get(w_n[0]);
            if(index>v.size())
              for(int i = v.size();i<index; i++)
                v.add(0.0);
            v.add(index, score);
            _temp.put(w_n[0], v);
          }
          else {
            Vector<Double> v = new Vector<>();
            for(int i = 0;i<index; i++)
              v.add(0.0);
            v.add(index, score);
            _temp.put(w_n[0], v);
          }
        }
      }
      Set<String> temp = _temp.keySet();
      for (String word : temp) {
        Vector<Double> v = _temp.get(word);
        double score = 0.0;
        double sum = 0.0;
        for (int i = 0; i < v.size(); i++) {
          score += ((double) 1 / (double) (i + 1)) * v.get(i);
        }
        for (int i = 1; i <= v.size(); i++) {
          sum += (double) 1 / (double) i;
        }
        score /= sum;
        _dict.put(word, score);
      }
    }
    catch(Exception e){
      System.err.println("Error while initializing SentiWordNet" + e);
    }
  }

  //from http://stackoverflow.com/a/15653183/2418202
  public Double extract(String word) {
    Double total = new Double(0);
    int count = 0;
    if (_dict.get(word + "#n") != null) {
      total = _dict.get(word + "#n") + total;
      ++count;
    }
    if(_dict.get(word+"#a") != null) {
      total = _dict.get(word + "#a") + total;
      ++count;
    }
    if(_dict.get(word+"#r") != null) {
      total = _dict.get(word + "#r") + total;
      ++count;
    }
    if(_dict.get(word+"#v") != null) {
      total = _dict.get(word + "#v") + total;
      ++count;
    }
    return count == 0 ? 0 : 100 * (total / count);
  }
}