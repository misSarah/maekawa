import java.util.Random;

/**
 * Created by micha on 3/24/2016.
 */
public class ExponentialDelay {

    private double meanIRDelay, meanCSDelay;
    private Random irDelayRandom = new Random(), csDelayRandom = new Random();

    public ExponentialDelay(int meanIRDelay, int meanCSDelay) {
        this.meanIRDelay = 1.0 / (double)meanIRDelay;
        this.meanCSDelay = 1.0 / (double)meanCSDelay;
    }

    public int getIRDelay() {
        return ExponentialRandomNumber(this.irDelayRandom, meanIRDelay);
    }

    public long getCSDelay() {
        return ExponentialRandomNumber(this.csDelayRandom, meanCSDelay);
    }

    private static int ExponentialRandomNumber(Random rand, double mean) {
        double randVal = Math.log(1 - rand.nextDouble()) / (0.0 - mean);
        return (int) Math.ceil(randVal);
    }

    @Override
    public String toString() {
        return String.format("{ IR: %d, CS: %d }", (int) (1.0 / this.meanIRDelay), (int) (1.0 / this.meanCSDelay));
    }

}
