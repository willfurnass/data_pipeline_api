package uk.ramp.objects;

public class NumericalArrayImpl implements NumericalArray {
  private final Object[] nDArray;

  public NumericalArrayImpl(Object[] nDArray) {
    this.nDArray = nDArray;
  }

  @Override
  public Number[] as1DArray() {
    return (Number[]) nDArray;
  }

  @Override
  public Number[][] as2DArray() {
    return (Number[][]) nDArray;
  }

  @Override
  public Number[][][] as3DArray() {
    return (Number[][][]) nDArray;
  }

  @Override
  public Number[][][][] as4DArray() {
    return (Number[][][][]) nDArray;
  }

  @Override
  public Number[][][][][] as5DArray() {
    return (Number[][][][][]) nDArray;
  }
}
