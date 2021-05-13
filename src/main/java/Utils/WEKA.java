package Utils;

import weka.*;
import weka.classifiers.evaluation.Evaluation;
import weka.classifiers.trees.J48;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;
import java.util.Random;

public class WEKA {
    public static void main (String[] args) throws Exception {
        DataSource source = new DataSource("relatedness_small.arff");
        Instances data = source.getDataSet();

        if (data.classIndex() == -1)
            data.setClassIndex(data.numAttributes() - 1);

        String[] options = new String[1];
        options[0] = "-U";
        J48 tree = new J48();
        tree.setOptions(options);
        tree.buildClassifier(data);

        Evaluation eval = new Evaluation(data);
        eval.crossValidateModel(tree, data, 10, new Random(1));

        System.out.println("Weighted precision: " + eval.weightedPrecision());
        System.out.println("Weighted recall: " + eval.weightedRecall());
        System.out.println("Weighted F-Measure: " + eval.weightedFMeasure());

        System.out.println();

        System.out.println(eval.toMatrixString());
    }
}
