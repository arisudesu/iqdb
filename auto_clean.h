/***************************************************************************\
    Design patterns to automatically clean up when a variable goes out of scope.

    Copyright (C) 2008 piespy@gmail.com

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
\**************************************************************************/

/* 1. Object version. Call given member function when going out of scope.
      For when always cleaning in the destructor is not desired.

   Example:
   typedef AutoClean<Foo, &Foo::clean> CleanFoo;

   Calls Foo::clean when the CleanFoo object goes out of scope.

   2. Pointer version. Deletes pointee when going out of scope and trusts
      destructor to do the cleaning.

   Example:
   typedef AutoCleanPtr<Foo> CleanFoo;

   Also supports object.set(foo2) to delete the old pointer (if any) and use
   the new one (which may be NULL).

   2b. Array version. Uses delete[] instead of delete.
*/

template<typename T, void (T::*cleanup_func)()>
class AutoClean : public T {
public:
	AutoClean() { }
	AutoClean(const T& v) : T(v) { }
	~AutoClean() { (this->*cleanup_func)(); }

private:
	AutoClean(const AutoClean&);
	AutoClean& operator = (const AutoClean&);
};

template<typename T>
class AutoCleanPtr {
public:
	AutoCleanPtr() : m_p(NULL) { }
	AutoCleanPtr(T* p) : m_p(p) { }
	~AutoCleanPtr() { delete m_p; }

	void set(T* p) { delete m_p; m_p = p; }
	T* ptr() { return m_p; }

	T& operator* () { return *m_p; }
	T* operator->() { return m_p; }

private:
	AutoCleanPtr(const AutoCleanPtr&);
	AutoCleanPtr& operator = (const AutoCleanPtr&);

	T* m_p;
};

template<typename T>
class AutoCleanArray {
public:
	AutoCleanArray() : m_p(NULL) { }
	AutoCleanArray(T* p) : m_p(p) { }
	~AutoCleanArray() { delete[] m_p; m_p = NULL; }

	void set(T* p) { delete[] m_p; m_p = p; }
	T* ptr() { return m_p; }

	T& operator[](size_t ind) { return m_p[ind]; }

	T& operator* () { return *m_p; }
	T* operator->() { return m_p; }

private:
	AutoCleanArray(const AutoCleanArray&);
	AutoCleanArray& operator = (const AutoCleanArray&);

	T* m_p;
};

