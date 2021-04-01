/***************************************************************************\
    Variant of std::vector<size_t> that efficiently stores small differences.

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

#include <vector>

static const size_t size_t_mask = sizeof(size_t) - 1;

union delta_value {
	delta_value() { }
	delta_value(size_t v) : full(v) { }

	size_t full;
	unsigned char sub[size_t_mask + 1];
};

struct delta_iterator : public std::iterator<std::forward_iterator_tag, size_t> {
	delta_iterator(const delta_value* bitr) : m_itr(bitr), m_val(*bitr), m_bval(0), m_ind(0) { ++m_itr; }
	delta_iterator(const delta_value* bitr, int ind) : m_itr(bitr), m_ind(ind) { }
	delta_iterator() { }

	size_t operator*() const;
	delta_iterator& operator++();
	bool operator==(const delta_iterator& other) const { return m_itr == other.m_itr && m_ind == other.m_ind; }
	bool operator!=(const delta_iterator& other) const { return m_itr != other.m_itr || m_ind != other.m_ind; }

private:
	const delta_value* m_itr;
	delta_value m_val;
	size_t m_bval;
	int m_ind;
};

class delta_queue {
protected:
	static const size_t mask = size_t_mask;
	typedef std::vector<delta_value> base_type;
	base_type m_base;

public:
	typedef delta_iterator iterator;
	typedef delta_iterator const_iterator;

	delta_queue() : m_base(1, 0), m_size(0), m_pos(0), m_bval(0) { }

	const_iterator begin() const { return const_iterator(&*m_base.begin()); }
	const_iterator end() const { return const_iterator(&*m_base.end(), m_size & mask); }

	// Reserve storage under the assumption most values will be <255 and fit in one byte.
	// Set fixed=true if you know the exact storage size required, then the size argument
	// is the storage size rather than the number of values.
	void reserve(size_t size, bool fixed = false) { m_base.reserve(fixed ? size : size * 129 / 512 + 1); }

	void push_back(size_t v);

	bool empty() { return !m_size; }
	size_t size() const { return m_size; }

	void swap(delta_queue& other);

	size_t base_capacity() const { return m_base.capacity(); }
	size_t base_size() const { return m_base.size(); }

private:
	size_t m_size;
	size_t m_pos;
	size_t m_bval;
};

inline size_t delta_iterator::operator*() const {
	unsigned char val = m_val.sub[m_ind];
//fprintf(stderr, "Returning %zd+%d [ind=%d val=%08x full=%08x]. ", m_bval, val, m_ind, m_val.full, full);
	return m_bval + (val == 255 ? m_itr->full : val);
}

inline delta_iterator& delta_iterator::operator++() {
	unsigned char val = m_val.sub[m_ind];
//fprintf(stderr, "Advancing, from base=%zd ind=%d val=%08x ", m_bval, m_ind, m_val.full);
	m_bval += val == 255 ? m_itr++->full : val;
	m_ind++;

	// Zero-branch advancing of base iterator.
	// adv_mask is -1=0xffff.. if not reading a new set of value bytes, or 0 otherwise.
	size_t adv_mask = (m_ind & ~size_t_mask) / (size_t_mask + 1) - 1;
	m_ind &= size_t_mask;
	m_val.full = adv_mask & m_val.full | ~adv_mask & m_itr->full;
	m_itr += adv_mask + 1;
//fprintf(stderr, "to base=%zd ind=%d val=%08x. ", m_bval, m_ind, m_val.full);
	return *this;
}

inline void delta_queue::swap(delta_queue& other) {
	m_base.swap(other.m_base);
	std::swap(m_size, other.m_size);
	std::swap(m_pos, other.m_pos);
	std::swap(m_bval, other.m_bval);
}

inline void delta_queue::push_back(size_t v) {
	v -= m_bval;
	m_bval += v;

	if (v >= 255)
		m_base.push_back(v);

//fprintf(stderr, "Writing %zd at %zd/%zd. ", v, m_pos, m_size & mask);
	m_base[m_pos].sub[m_size++ & mask] = v < 255 ? v : 255;
//fprintf(stderr, "Now size=%zd. ", m_size);

	if (!(m_size & mask)) {
		m_pos = m_base.size();
		m_base.push_back(0);
//fprintf(stderr, "Now pos=%zd. ", m_pos);
	};
}

