package utils

import (
	"math/rand"
)

// Graph is a generic graph data structure.
type Graph[V comparable] struct {
	vertices  []V
	adjacency [][]int
}

// VertexGenerator is a function that deterministically generates a vertex of a graph from an integer ID.
type VertexGenerator[V comparable] func(id int) (vertex V)

func convertIDsToGraph[V comparable](graph [][]int, gen VertexGenerator[V]) Graph[V] {
	vertices := make([]V, len(graph))
	for i := 0; i < len(graph); i++ {
		vertices[i] = gen(i)
	}
	adjacency := make([][]int, len(graph))
	for i := 0; i < len(graph); i++ {
		adjacency[i] = make([]int, len(graph[i]))
		copy(adjacency[i], graph[i])
	}
	return Graph[V]{vertices, adjacency}
}

// GenCliqueGraph generates a clique graph with n vertices.
func GenCliqueGraph[V comparable](n int, gen VertexGenerator[V]) Graph[V] {
	graphValues := make([][]int, n)
	for i := 0; i < n; i++ {
		graphValues[i] = make([]int, 0)
	}
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			graphValues[i] = append(graphValues[i], j)
			graphValues[j] = append(graphValues[j], i)
		}
	}

	return convertIDsToGraph(graphValues, gen)
}

// GenLineGraph generates a line graph with n vertices.
func GenLineGraph[V comparable](n int, gen VertexGenerator[V]) Graph[V] {
	graphValues := make([][]int, n)
	for i := 0; i < n; i++ {
		graphValues[i] = make([]int, 0)
	}
	for i := 0; i < n-1; i++ {
		graphValues[i] = append(graphValues[i], i+1)
		graphValues[i+1] = append(graphValues[i+1], i)
	}

	return convertIDsToGraph(graphValues, gen)
}

// GenCycleGraph generates a cycle graph with n vertices.
func GenCycleGraph[V comparable](n int, gen VertexGenerator[V]) Graph[V] {
	graphValues := make([][]int, n)
	for i := 0; i < n; i++ {
		graphValues[i] = make([]int, 0)
	}
	for i := 0; i < n; i++ {
		graphValues[i] = append(graphValues[i], (i+1)%n)
		graphValues[(i+1)%n] = append(graphValues[(i+1)%n], i)
	}

	return convertIDsToGraph(graphValues, gen)
}

// GenRandomTreeGraph generates a random tree graph with n vertices.
func GenRandomTreeGraph[V comparable](n int, gen VertexGenerator[V]) Graph[V] {
	graphValues := make([][]int, n)
	for i := 0; i < n; i++ {
		graphValues[i] = make([]int, 0)
	}
	numAdded := 1

	for numAdded < n {
		newVertex := numAdded
		parent := rand.Intn(numAdded)
		graphValues[newVertex] = append(graphValues[newVertex], parent)
		graphValues[parent] = append(graphValues[parent], newVertex)
		numAdded++
	}

	g := convertIDsToGraph(graphValues, gen)
	if !g.IsConnected() {
		panic("I cannot generate a connected tree graph")
	}
	return g
}

// GenRandomGraph generates a random graph with n vertices.
func GenRandomGraph[V comparable](n int, gen VertexGenerator[V]) Graph[V] {
	g := func() Graph[V] {
		graph := make([][]int, n)
		for i := 0; i < n; i++ {
			graph[i] = make([]int, 0)
		}
		for i := 0; i < n; i++ {
			for j := i + 1; j < n; j++ {
				if rand.Intn(2) == 1 {
					graph[i] = append(graph[i], j)
					graph[j] = append(graph[j], i)
				}
			}
		}
		return convertIDsToGraph(graph, gen)
	}
	for {
		graph := g()
		if graph.IsConnected() {
			return graph
		}
	}
}

func (graph Graph[V]) IsConnected() bool {
	visited := make([]bool, len(graph.vertices))
	var dfs func(int)
	dfs = func(v int) {
		visited[v] = true
		for _, u := range graph.adjacency[v] {
			if !visited[u] {
				dfs(u)
			}
		}
	}
	dfs(0)
	for _, v := range visited {
		if !v {
			return false
		}
	}
	return true
}

func (graph Graph[V]) GetNeighbors(v V) []V {
	for i, vertex := range graph.vertices {
		if vertex == v {
			neighbors := make([]V, len(graph.adjacency[i]))
			for j, neighbor := range graph.adjacency[i] {
				neighbors[j] = graph.vertices[neighbor]
			}
			return neighbors
		}
	}
	return nil
}

func (graph Graph[V]) GetVertices() []V {
	copyVertices := make([]V, len(graph.vertices))
	copy(copyVertices, graph.vertices)
	return copyVertices
}

func (graph Graph[V]) GetSize() int {
	return len(graph.vertices)
}
