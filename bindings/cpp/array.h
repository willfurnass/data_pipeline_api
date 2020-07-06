// copyright, Alex Meakins and Qingfeng Xia  @ UKAEA
// it is released under open source like BSD
// https://github.com/simple-access-layer/source
// this code has been widely unit tested, unit test code can be copied here soon

#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include <array>
#include <type_traits>
#include <stdexcept>

#include "pybind11/numpy.h"
#include "pybind11/stl.h"
namespace py = pybind11;

/// HDF5 C-API has DimensionScaled, but not used in HDF5 IO
template <typename DT>
struct Dimension
{
    std::string title;
    std::vector<std::string> names; // tick names as in matplotlib, optional
    std::vector<DT> values;         // ticks can be a better name
    std::string units;
};

/// a typedef to ease future refactoring on data structure
using ShapeType = std::vector<uint64_t>;

/// base class for all ArrayT<T> as non-templated interface to array metadata
class Array
{
public:
    typedef std::shared_ptr<Array> Ptr;
    Array(ShapeType _shape)
    {

        this->m_dimension = _shape.size();
        this->m_shape = _shape;
        this->m_strides.resize(this->m_dimension);

        // calculate strides
        int16_t i = this->m_dimension - 1;
        this->m_strides[i] = 1;
        i--;
        while (i >= 0)
        {
            this->m_strides[i] = this->m_strides[i + 1] * this->m_shape[i + 1];
            i--;
        }
    }

    virtual ~Array() // virtual destructor when virtual functions are present.
    {
    }
    /// those functions below could be made non-virtual for better performance
    inline ShapeType shape() const
    {
        return this->m_shape;
    };
    /// consider: plural name
    inline size_t dimension() const
    {
        return this->m_shape.size();
    };
    inline ShapeType strides() const
    {
        return this->m_strides;
    };

    std::string &units()
    {
        return m_unit;
    }
    const std::string &units() const
    {
        return m_unit;
    }

    //virtual AttributeType element_type() const = 0;
    virtual std::string element_type_name() const = 0;

    /// @{
    /** infra-structure for C-API */
    virtual uint64_t size() const = 0;

    /// numpy array's nbytes()
    virtual size_t byte_size() const = 0;

    /// read-only pointer to provide read view into the data buffer
    virtual const void *data_pointer() const = 0;

    /// modifiable raw pointer to data buffer, use it with care
    virtual void *data_pointer() = 0;

    virtual void *data_at(int i0, int64_t i1 = -1, int64_t i2 = -1, int64_t i3 = -1, int64_t i4 = -1,
                          int64_t i5 = -1, int64_t i6 = -1, int64_t i7 = -1, int64_t i8 = -1,
                          int64_t i9 = -1) = 0;

    virtual void encode(py::object &group) const = 0;
    /// @}
protected:
    uint8_t m_dimension; // CONSIDER: size_t otherwise lots of compiler warning
    ShapeType m_shape;
    ShapeType m_strides;
    std::string m_unit;

public:
    template <typename DT>
    static const char *to_dtype_name()
    {
        /// TODO: using DT = std::remove_cv<DType>::type;
        /// decay() from pointer or reference to type

        /// NOTE: std::byte, std::complex, not necessary
        /// `if constexpr ()` or constexpr template is only for C++17
        if (std::is_same<DT, int64_t>::value)
            return "int64";
        else if (std::is_same<DT, int32_t>::value)
            return "int32";
        else if (std::is_same<DT, int16_t>::value)
            return "int16";
        else if (std::is_same<DT, int8_t>::value)
            return "int8";
        else if (std::is_same<DT, uint64_t>::value)
            return "uint64";
        else if (std::is_same<DT, uint32_t>::value)
            return "uint32";
        else if (std::is_same<DT, uint16_t>::value)
            return "uint16";
        else if (std::is_same<DT, uint8_t>::value)
            return "uint8";
        else if (std::is_same<DT, float>::value)
            return "float32";
        else if (std::is_same<DT, double>::value)
            return "float64";
        else if (std::is_same<DT, bool>::value)
            return "bool";
        // else if (std::is_same<DT, std::string>::value)
        //     return "string";   // not supported, so disabled here
        else
        {
            throw std::runtime_error("data type valid as Array element or Atomic value");
        }
    }
};

/**
It is a multi-dimension array with dat saved to a flattend std::vector<T>
No default constructor without parameter is allowed, at least give the shape
 of the array,  `std::vector<uint64_t>`, as the first parameter 
consistent with python `numpy.array' dtype's name.     

A set of typedefs are provided that define the supported array types. 

Examples of array creation:
```c++
    // create a 1D uint8 array with 1000 elements with default value 0.
    UInt8Array a1({1000});  // ArrayT<uint8_t>

    // create a 2D int32 array with 50x20 elements with default value 0.
    Int32Array a2({50, 20});  // ArrayT<int>

    // create a 3D float array with 512x512x3 elements.
    Float32Array a3({512, 512, 3});

    // create Array from a buffer with shape vector as the first parameter
    double buf[] = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
    Float64Array a4({2, 3}, buf);

    // create Array from a flattened vector with shape vector as the first parameter
    Float64Array a5({2, 3}, std::vector<double>({1.0, 2.0, 3.0, 4.0, 5.0, 6.0}));

    // 2D matrix from const `std::vector<std::vector<T>>&` row-major
    std::vector<std::vector<double>> mat = {{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}};
    Float64Array a6(mat);
```
*/
template <class T, typename = std::enable_if<!std::is_same<bool, T>::value>>
class ArrayT : public Array
{

protected:
    /// this non-public constructor should be called by other public constructors
    ArrayT(ShapeType _shape, const std::string _element_type_name)
        : Array(_shape), m_element_type_name(_element_type_name)
    {
        // calculate array buffer length
        uint64_t element_size = 1;
        for (uint64_t d : this->m_shape)
            element_size *= d;

        this->m_data.resize(element_size);
    }

public:
    typedef std::shared_ptr<ArrayT<T>> Ptr;
    typedef T value_type;

    /*
    Array constructor.

    Initialises an array with the specified dimensions (shape). The
    array shape is a vector defining the length of each dimensions
    of the array. The number of elements in the shape vector
    defines the number of dimensions.
    */
    ArrayT(ShapeType _shape)
        : ArrayT(_shape, Array::to_dtype_name<T>())
    {
    }

    /// move constructor by take over (steal, move) the content of input vector
    ArrayT(ShapeType _shape, std::vector<T> &&vec)
        : ArrayT(_shape, Array::to_dtype_name<T>())
    {
        this->m_data = vec;
    }

    ArrayT(ShapeType _shape, const std::vector<T> &vec)
        : ArrayT(_shape, Array::to_dtype_name<T>())
    {
        this->m_data = vec;
    }

    /// create Array from a  1D buffer with shape vector as the first parameter
    ArrayT(ShapeType _shape, const T *buf)
        : Array(_shape, Array::to_dtype_name<T>())
    {
        size_t ec = 1; // std::inner_product()
        for (const auto &s : _shape)
        {
            ec *= s;
        }
        std::copy_n(buf, ec, m_data.begin());
    }

    /// 2D matrix from const `std::vector<std::vector<T>>&` row-major
    ArrayT(const std::vector<std::vector<T>> &mat)
        : Array({mat.size(), mat[0].size()}, Array::to_dtype_name<T>())
    {
        auto offset = m_data.begin();
        for (size_t r = 0; r < mat.size(); r++)
        {
            size_t col = mat[r].size();
            std::copy_n(mat[r].cbegin(), col, offset);
            offset += col;
        }
    }

    // CONSIDER: disable those constructors, force shared_ptr<>
    //            Array(const Array&);
    //            Array& operator= (const Array&);
    //            Array(Array&&);
    //            Array& operator= (Array&&);

    virtual ~ArrayT(){};

    inline virtual std::string element_type_name() const
    {
        return m_element_type_name;
    }

    std::string &dim_unit(int i)
    {
        if (m_dims.size() < i + 1)
            m_dims.push_back(Dimension<T>());
        return m_dims[i].units;
    }
    // must exist before read this field
    const std::string &dim_unit(int i) const
    {
        return m_dims[i].units;
    }

    std::string &dim_title(int i)
    {
        if (m_dims.size() < i + 1)
            m_dims.push_back(Dimension<T>());
        return m_dims[i].title;
    }
    const std::string &dim_title(int i) const
    {
        return m_dims[i].title;
    }

    std::vector<T> &dim_values(int i)
    {
        if (m_dims.size() < i + 1)
            m_dims.push_back(Dimension<T>());
        return m_dims[i].values;
    }
    const std::vector<T> &dim_values(int i) const
    {
        return m_dims[i].values;
    }

    std::vector<std::string> &dim_names(int i)
    {
        if (m_dims.size() < i + 1)
            m_dims.push_back(Dimension<T>());
        return m_dims[i].names;
    }
    const std::vector<std::string> &dim_names(int i) const
    {
        return m_dims[i].names;
    }

    template <typename DT = T>
    std::vector<Dimension<DT>> &dims()
    {
        return m_dims;
    }
    template <typename DT = T>
    const std::vector<Dimension<DT>> &dims() const
    {
        return m_dims;
    }
    /// @{ STL container API
    /*
            Returns the length of the array buffer, element_size, not byte size
            flattened 1D array from all dimensions
            */
    inline virtual uint64_t size() const
    {
        return this->m_data.size();
    };

    /// todo: STL iterators
    /// @}

    /// @{ Infrastructure for C-API

    /// data buffer byte size, equal to numpy.nbytes()
    inline virtual size_t byte_size() const
    {
        /// NOTE: std::vector<bool> stores bit instead of byte for each element
        return this->m_data.size() * sizeof(T);
    };

    /// read-only `const void*` pointer to provide read view into the data buffer
    inline virtual const void *data_pointer() const
    {
        // std::enable<> does not work for virtual function, so must check at runtime
        if (element_type_name() != "string")
            return this->m_data.data();
        else // Array<String> buffer addess does not contains contents but addr to content
        {
            throw std::runtime_error("Should not use Array<String>::data_pointer()");
        }
    }

    /// modifiable raw pointer to data buffer, use it with care
    inline virtual void *data_pointer()
    {
        if (element_type_name() != "string") // std::enable<> does not work for virtual function
            return this->m_data.data();
        else
        {
            throw std::runtime_error("Should not use Array<String>::data_pointer()");
        }
    }

    /// using array as index can be more decent
    inline virtual void *data_at(int i0, int64_t i1 = -1, int64_t i2 = -1, int64_t i3 = -1, int64_t i4 = -1,
                                 int64_t i5 = -1, int64_t i6 = -1, int64_t i7 = -1, int64_t i8 = -1,
                                 int64_t i9 = -1) override
    {
        return std::addressof(this->at(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9));
    }
    /// @}

    /*
            Fast element access via direct indexing of the array buffer (flattened ID array).

            The Array holds the data in a 1D strided array. Indexing into
            multidimensional arrays therefore requires the user to
            appropriately stride across the data. See the stride attribute.

            No bounds checking is performed.
            */
    inline T &operator[](const uint64_t index)
    {
        return this->m_data[index];
    };

    inline const T &operator[](const uint64_t index) const
    {
        return this->m_data[index];
    };

    /// expose reference to the underneath data container (std::vector)
    inline std::vector<T> &values()
    {
        return this->m_data;
    };
    /// expose the const reference to the underneath data container (std::vector)
    inline const std::vector<T> &values() const
    {
        return this->m_data;
    };

    // C++14 provide <T indices ...>

    /// quickly access an element for 2D matrix row and col,  without bound check
    /// `array(row_index, col_index)`  all zero for the first element
    inline T &operator()(const uint64_t row, const uint64_t column)
    {
        // assert(m_dimension == 2);
        uint64_t index = row * this->m_strides[0] + column;
        return this->m_data[index];
    };

    inline const T &operator()(const uint64_t row, const uint64_t column) const
    {
        uint64_t index = row * this->m_strides[0] + column;
        return this->m_data[index];
    };

    /*
            Access an element of the array.

            std::vector<T> has two versions
            reference at (size_type n);
            const_reference at (size_type n) const;

            This method performs bounds checking and accepts a variable
            number of array indices corresponding to the dimensionality of
            the array.

            Data access is slower than direct buffer indexing, however it
            handles striding for the user.

            Due to the method of implementing this functionality in C++, it
            only supports arrays with a maximum of 10 dimensions.
            */
    virtual T &at(int i0, int64_t i1 = -1, int64_t i2 = -1, int64_t i3 = -1, int64_t i4 = -1, int64_t i5 = -1,
                  int64_t i6 = -1, int64_t i7 = -1, int64_t i8 = -1, int64_t i9 = -1) throw()
    {
        // NOTE: index are signed integer, assigning -1 means max unsigned interger
        if (this->m_dimension > 10)
        {
            throw std::out_of_range("The at() method can only be used with arrays of 10 dimensions of less.");
        }

        // convert the list or arguments to an array for convenience
        std::array<int64_t, 10> dim_index = {i0, i1, i2, i3, i4, i5, i6, i7, i8, i9};

        uint64_t element_index = 0;
        for (uint8_t i = 0; i < this->m_dimension; i++)
        {
            // check the indices are inside the array bounds
            if ((dim_index[i] < 0) || (static_cast<uint64_t>(dim_index[i]) > this->m_shape[i] - 1UL))
            {
                throw std::out_of_range("An array index is missing or is out of bounds.");
            }

            element_index += dim_index[i] * this->m_strides[i];
        }

        return this->m_data[element_index];
    }

    /// write pybind::array with name `array` inside the given H5Group
    virtual void encode(py::object &group) const override
    {
        using namespace pybind11::literals;
        py::dtype dt = py::dtype::of<T>();
        //py::list s = py::cast(this->shape());
        ShapeType _strides; // empty as the default, C_stride
        const py::array pya(dt, this->m_shape, _strides, this->m_data.data());

        py::tuple shape = py::cast(m_shape);
        py::object dataset = group.attr("require_dataset")(py::str("array"),
                                                           "shape"_a = shape, "dtype"_a = dt);
        dataset.attr("write_direct")(pya);

        //group.attr("__setitem__")(py::str("array"), pya);  // equal to the 3 lines above

        /// read meta data for the array, currently, it is attached to group as dataset
        /// it may also be possible to attached to a DataSet's AttributeProxy
        // py::object attrs = array_dataset.attr("attrs");
        encode_metadata(group);
    }

    void encode_metadata(py::object &attrs) const
    {
        using namespace pybind11::literals;
        //py::module np = py::module::import("numpy");
        py::module h5py = py::module::import("h5py");

        attrs.attr("__setitem__")(py::str("units"), units());
        for (size_t i = 0; i < m_dims.size(); i++)
        {
            std::string dn = "Dimension_" + std::to_string(i);
            py::array _dv = py::cast(m_dims[i].values);
            attrs.attr("__setitem__")(py::str(dn + "_values"), _dv);
            attrs.attr("__setitem__")(py::str(dn + "_units"), py::cast(m_dims[i].units));
            py::str dtitle = py::cast(m_dims[i].title);
            attrs.attr("__setitem__")(py::str(dn + "_title"), dtitle);
            if (m_dims[i].names.size() > 0)
            {
                auto dt = h5py.attr("string_dtype")(); // encoding='utf-8'
                py::tuple s = py::cast(std::vector<size_t>({m_dims[i].names.size(), 1}));
                auto ds = attrs.attr("create_dataset")(py::str(dn + "_names"),
                                                       s, "dtype"_a = dt);
                int ind = 0;
                for (const auto &it : m_dims[i].names)
                {
                    ds.attr("__setitem__")(ind, py::str(it));
                    ind++;
                }

                // TypeError: Object dtype dtype('O') has no native HDF5 equivalent
                //py::array dnames = np.attr("array")(_dnames, "dtype"_a = py::dtype("object"));

                // Chris python data pipeline impl, but it does not work in C++
                //py::array encoded_names = np.attr("char").attr("encode")(_dnames);

                //py::array dnames(_dnames, "dtype"_a = py::dtype("object"));  // C++ has no such ctor

                //attrs.attr("__setitem__")(py::str(dn + "_names"), encoded_names);
            }
        }
    }

    static typename ArrayT<T>::Ptr decode_array(const py::array pya)
    {
        // todo: detect if numpy array is C row-major, then memcpy
        /// NOTE: all size dim in python are signed integer!, ssize_t
        ShapeType shape(pya.ndim());
        for (size_t i = 0; i < pya.ndim(); i++)
        {
            shape[i] = pya.shape()[i];
        }
        typename ArrayT<T>::Ptr arr = std::make_shared<ArrayT<T>>(shape);
        //std::cout << "pya.itemsize() = " << pya.itemsize() << std::endl; // sizeof(element_type)
        //std::cout << "pya.size() = " << pya.size() << std::endl;  // this is element count
        std::copy((T *)(pya.data()), (T *)(pya.data()) + pya.size(), arr->m_data.begin());
        return arr;
    }

    static void decode_metadata(const py::object group, typename ArrayT<T>::Ptr arr)
    {
        using namespace pybind11::literals;
        py::module h5py = py::module::import("h5py");

        /// read meta data for the array, currently, it is attached to group as dataset
        /// it may also be possible to attached to a DataSet's AttributeProxy
        // py::object attrs = array_dataset.attr("attrs");
        py::object attrs = group;
        py::str _unit = attrs.attr("__getitem__")(py::str("units"));
        arr->units() = _unit;

        py::module np = py::module::import("numpy");

        for (size_t i = 0; i < arr->shape().size(); i++)
        {
            std::string dn = "Dimension_" + std::to_string(i);
            py::str dtitle = attrs.attr("__getitem__")(py::str(dn + "_title"));
            py::array _dv = attrs.attr("__getitem__")(py::str(dn + "_values"));
            py::str dunit = attrs.attr("__getitem__")(py::str(dn + "_units"));
            std::vector<std::string> _dnames;
            size_t sz = _dv.size();
            /// NOTE: if (e) is alwasy true
            auto e = attrs.attr("__contains__")(py::str(dn + "_names"));
            if (py::bool_(e).cast<bool>())
            {
                auto dt = h5py.attr("string_dtype")(); // encoding='utf-8'
                py::tuple s = py::cast(std::vector<size_t>({sz, 1}));
                auto ds = attrs.attr("require_dataset")(py::str(dn + "_names"),
                                                        "shape"_a = s, "dtype"_a = dt);

                for (int ind = 0; ind < sz; ind++)
                {
                    std::string s = py::str(ds.attr("__getitem__")(ind)).cast<std::string>();
                    _dnames.push_back(s);
                }
            }
            Dimension<T> d{dtitle, _dnames, _dv.cast<std::vector<T>>(), dunit};
            arr->dims().push_back(d);
        }
    }

    static typename ArrayT<T>::Ptr decode(const py::object group)
    {
        using namespace pybind11::literals;
        auto dataset = group.attr("__getitem__")(py::str("array"));

        py::module np = py::module::import("numpy");
        const py::array pya = np.attr("zeros")(dataset.attr("shape"),
                                               "dtype"_a = dataset.attr("dtype"));
        dataset.attr("read_direct")(pya);

        auto arr = ArrayT<T>::decode_array(pya);
        decode_metadata(group, arr);

        return arr;
    }

protected:
    // change element type is not possible without re-create the array object
    const std::string m_element_type_name;
    std::vector<T> m_data;
    std::vector<Dimension<T>> m_dims;
};

/// typedef naming as Javascript TypedArray
typedef ArrayT<int8_t> Int8Array;
typedef ArrayT<int16_t> Int16Array;
typedef ArrayT<int32_t> Int32Array;
typedef ArrayT<int64_t> Int64Array;

typedef ArrayT<uint8_t> UInt8Array;
typedef ArrayT<uint16_t> UInt16Array;
typedef ArrayT<uint32_t> UInt32Array;
typedef ArrayT<uint64_t> UInt64Array;

typedef ArrayT<float> Float32Array;
typedef ArrayT<double> Float64Array;

/** `typedef ArrayT<bool> BoolArray` will not work, should be disabled
 * Reasons
 * + std::vector<bool> is a specialized std::vector<T>, each element use a bit not byte
 * + all left reference to element will not work/compile, such as `T& operator []`
 *
 * `typedef ArrayT<uint8_t> BoolArray;` will not give correct element type
 * A new type BoolArray should be defined, as a derived class of Array<uint8_t>
 * Solution: `class BoolArray : public Array<uint8_t>`
 * override the constructor solved the element_type_name initialization
 * */
class BoolArray : public ArrayT<uint8_t>
{
public:
    typedef std::shared_ptr<BoolArray> Ptr;
    BoolArray(const ShapeType _shape)
        : ArrayT<uint8_t>(_shape, "bool")
    {
    }

    /// this ctor is same as ArrayT<uint8_t>
    BoolArray(const ShapeType _shape, const std::vector<uint8_t> vec)
        : ArrayT<uint8_t>(_shape, "bool")
    {
        for (size_t i = 0; i < vec.size(); i++)
        {
            m_data[i] = vec[i];
        }
    }

    BoolArray(const ShapeType _shape, const std::vector<bool> vec)
        : ArrayT<uint8_t>(_shape, "bool")
    {
        for (size_t i = 0; i < vec.size(); i++)
        {
            m_data[i] = vec[i];
        }
    }
    /// write to HDF5 is done,  TODO: test read back from HDF5
};

class DataDecoder
{
public:
    /// decode array without knowing the element type
    static Array::Ptr decode_array(const py::array &pyo)
    {

        /** attribute identifier strings in encoded pyo objects
             those type name should equal to `numpy.typename`
            see: https://numpy.org/devdocs/user/basics.types.html
        */
        static char TYPE_NAME_INT8[] = "int8";
        static char TYPE_NAME_INT16[] = "int16";
        static char TYPE_NAME_INT32[] = "int32";
        static char TYPE_NAME_INT64[] = "int64";
        static char TYPE_NAME_UINT8[] = "uint8";
        static char TYPE_NAME_UINT16[] = "uint16";
        static char TYPE_NAME_UINT32[] = "uint32";
        static char TYPE_NAME_UINT64[] = "uint64";
        static char TYPE_NAME_FLOAT32[] = "float32";
        static char TYPE_NAME_FLOAT64[] = "float64";
        static char TYPE_NAME_BOOL[] = "bool";
        static char TYPE_NAME_STRING[] = "string"; // may not support

        auto dataset = pyo.attr("__getitem__")(py::str("array"));

        auto dt = dataset.attr("dtype")();
        std::string el_type_name = py::str(dt.attr("name")).cast<std::string>();
        // py::print("el_type_name = ", el_type_name);  // correct here

        // this can be removed if Array<T> is working
        if (el_type_name == TYPE_NAME_INT8)
            return Int8Array::decode(pyo);
        else if (el_type_name == TYPE_NAME_INT16)
            return Int16Array::decode(pyo);
        else if (el_type_name == TYPE_NAME_INT32)
            return Int32Array::decode(pyo);
        else if (el_type_name == TYPE_NAME_INT64)
            return Int64Array::decode(pyo);
        else if (el_type_name == TYPE_NAME_UINT8)
            return UInt8Array::decode(pyo);
        else if (el_type_name == TYPE_NAME_UINT16)
            return UInt16Array::decode(pyo);
        else if (el_type_name == TYPE_NAME_UINT32)
            return UInt32Array::decode(pyo);
        else if (el_type_name == TYPE_NAME_UINT64)
            return UInt64Array::decode(pyo);
        else if (el_type_name == TYPE_NAME_FLOAT32)
            return Float32Array::decode(pyo);
        else if (el_type_name == TYPE_NAME_FLOAT64)
            return Float64Array::decode(pyo);
        // else if (el_type_name == TYPE_NAME_BOOL)
        //    return BoolArray::decode(pyo); // TODO: does not compile
        else
            throw std::runtime_error("data type string `" + el_type_name + "` is not supported");
    }
};
